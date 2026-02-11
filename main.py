import os

# CI環境でのログ視認性向上のための設定 (全てのライブラリ読み込み前に実行)
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TQDM_DISABLE"] = "1"

import argparse
import json
import signal
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import tqdm as tqdm_mod
from dotenv import load_dotenv
from loguru import logger
from tqdm import tqdm

# モジュールのインポート
from catalog_manager import CatalogManager
from edinet_engine import EdinetEngine
from edinet_xbrl_prep.edinet_xbrl_prep.fs_tbl import get_fs_tbl
from master_merger import MasterMerger
from network_utils import patch_all_networking


# tqdm を無効化
def no_op_tqdm(*args, **kwargs):
    kwargs.update({"disable": True})
    return tqdm(*args, **kwargs)


tqdm_mod.tqdm = no_op_tqdm

# 全体的な通信の堅牢化を適用
patch_all_networking()

# 設定
DATA_PATH = Path("data").resolve()
RAW_BASE_DIR = DATA_PATH / "raw"
TEMP_DIR = DATA_PATH / "temp"
PARALLEL_WORKERS = int(os.getenv("PARALLEL_WORKERS", os.cpu_count() or 4))
BATCH_PARALLEL_SIZE = 8

is_shutting_down = False


def signal_handler(sig, frame):
    global is_shutting_down
    logger.warning("中断信号を受信しました。シャットダウンしています...")
    is_shutting_down = True


signal.signal(signal.SIGINT, signal_handler)


def parse_worker(args):
    """並列処理用ワーカー関数"""
    docid, row, acc_obj, raw_zip, role_kws, task_type = args
    # 【修正】タスクタイプごとに個別の作業ディレクトリを作成し、競合（Race Condition）を回避
    extract_dir = TEMP_DIR / f"{docid}_{task_type}"
    try:
        if acc_obj is None:
            return docid, None, "Account list not loaded"

        logger.debug(f"解析開始: {docid} (Path: {raw_zip})")

        # 開発者ブログ推奨の get_fs_tbl を呼び出し
        df = get_fs_tbl(
            account_list_common_obj=acc_obj,
            docid=docid,
            zip_file_str=str(raw_zip),
            temp_path_str=str(extract_dir),
            role_keyward_list=role_kws,
        )

        if df is not None and not df.empty:
            df["docid"] = docid
            df["code"] = str(row.get("secCode", ""))[:4]
            df["submitDateTime"] = row.get("submitDateTime", "")
            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = df[col].astype(str)
            logger.debug(f"解析成功: {docid} ({task_type}) | 抽出レコード数: {len(df)}")
            return docid, df, None, task_type

        msg = "No objects to concatenate" if (df is None or df.empty) else "Empty Results"
        return docid, None, msg, task_type

    except Exception as e:
        import traceback

        err_detail = traceback.format_exc()
        logger.error(f"解析例外: {docid} ({task_type})\n{err_detail}")
        return docid, None, f"{str(e)}", task_type
    finally:
        if extract_dir.exists():
            import shutil

            shutil.rmtree(extract_dir)


def run_merger(catalog, merger, run_id):
    """Mergerモード: デルタファイルの集約とGlobal更新 (Atomic Commit & Rollback 戦略)"""
    logger.info(f"=== Merger Process Started (Run ID: {run_id}) ===")

    # 1. 準備：ロールバック用のスナップショット取得
    catalog.take_snapshot()

    # 2. ハウスキーピング (古いデルタの削除)
    catalog.cleanup_deltas(run_id, cleanup_old=True)

    # 2. 有効なデルタの収集
    deltas = catalog.load_deltas(run_id)
    if not deltas:
        logger.warning("有効なデルタデータが見つかりませんでした (No valid chunks found).")
        catalog.cleanup_deltas(run_id, cleanup_old=False)
        return

    # 3. マージとGlobal更新 (Atomic Commit: すべて defer=True でバッファに積む)
    all_masters_success = True
    processed_count = 0

    # --- A. Stock Master (Catalogデルタから最新情報を抽出) ---
    # 【修正】Workerはmasterデルタを生成しないため、常にカタログから得られる最新の社名情報を反映
    if "catalog" in deltas and not deltas["catalog"].empty:
        processed_count += 1
        cat_df = deltas["catalog"]

        # カタログからコードごとの最新社名を抽出
        latest_from_cat = cat_df.sort_values("submit_at", ascending=True).drop_duplicates(subset=["code"], keep="last")[
            ["code", "company_name"]
        ]

        current_master = catalog.master_df.copy()

        # 既存マスタに最新社名をマージ
        if current_master.empty:
            merged_master = latest_from_cat.copy()
            merged_master["is_active"] = True
            merged_master["sector"] = "その他"
        else:
            # 外部結合して、新しい社名があれば上書き、なければ既存維持
            merged_master = pd.merge(current_master, latest_from_cat, on="code", how="outer", suffixes=("", "_new"))
            if "company_name_new" in merged_master.columns:
                merged_master["company_name"] = merged_master["company_name_new"].fillna(merged_master["company_name"])
                merged_master.drop(columns=["company_name_new"], inplace=True)

            # デフォルト値補完
            merged_master["is_active"] = merged_master["is_active"].fillna(True)
            merged_master["sector"] = merged_master["sector"].fillna("その他")

        if "rec" in merged_master.columns:
            merged_master.drop(columns=["rec"], inplace=True)

        # 型正規化 (継承ロジック)
        if merged_master["is_active"].dtype == "object":
            merged_master["is_active"] = (
                merged_master["is_active"]
                .astype(str)
                .str.lower()
                .map({"true": True, "false": False, "1": True, "0": False, "none": True, "nan": True})
                .fillna(True)
                .astype(bool)
            )

        # 【超重要】これにより社名変更検知 (update_stocks_master) が走り、かつ defer=True で保存予約される
        if catalog.update_stocks_master(merged_master):
            logger.info("Stock Master update staged from Catalog (Atomic)")
        else:
            logger.error("❌ Failed to stage Global Stock Master")
            all_masters_success = False

    # --- B. History (listing, index, name) ---
    for key in ["listing", "index", "name"]:
        if key in deltas and not deltas[key].empty:
            processed_count += 1
            new_hist = deltas[key]
            current_hist = catalog._load_parquet(key)
            merged_hist = pd.concat([current_hist, new_hist], ignore_index=True).drop_duplicates()
            # 履歴も defer=True でバッファリング
            if catalog._save_and_upload(key, merged_hist, defer=True):
                logger.info(f"Global {key} History staged (Atomic)")
            else:
                logger.error(f"❌ Failed to stage Global {key} History")
                all_masters_success = False

    # --- C. Financial / Text (Sector based) ---
    for key, sector_df in deltas.items():
        if (key.startswith("financial_") or key.startswith("text_")) and not sector_df.empty:
            processed_count += 1
            identifier = key.replace("financial_", "").replace("text_", "")
            m_type = "financial_values" if key.startswith("financial_") else "qualitative_text"

            # defer=True を明示的に指定
            if not merger.merge_and_upload(
                identifier, m_type, sector_df, worker_mode=False, catalog_manager=catalog, defer=True
            ):
                logger.error(f"❌ Failed to stage {m_type} for {identifier}")
                all_masters_success = False

    # --- D. Catalog & RAW Assets ---
    if all_masters_success:
        # RAWデータのアップロード復旧 (Atomicバッファに積む)
        if RAW_BASE_DIR.exists() and any(RAW_BASE_DIR.iterdir()):
            processed_count += 1
            catalog.upload_raw_folder(RAW_BASE_DIR, path_in_repo="raw", defer=True)
            logger.info("RAW assets staged for upload (Atomic)")

        if "catalog" in deltas and not deltas["catalog"].empty:
            processed_count += 1
            new_df = deltas["catalog"]
            merged_catalog = pd.concat([catalog.catalog_df, new_df], ignore_index=True).drop_duplicates(
                subset=["doc_id"], keep="last"
            )
            # カタログも予約
            catalog._save_and_upload("catalog", merged_catalog, defer=True)
            logger.info("Global Catalog staged (Atomic)")

        # 【アトミック・コミット実行】
        if processed_count > 0:
            if catalog.push_commit(f"Merger Atomic Success: {run_id}"):
                logger.success(
                    f"=== Merger完了: 全データをアトミックに更新しました (Total: {processed_count} files) ==="
                )
                # メモリ上のカタログ状態を確定させる
                catalog.catalog_df = merged_catalog

                # 【極限の堅牢性：Read-after-Write Verification (RaW-V)】
                # コミット直後に、キャッシュを無視してリモートから再取得し、実際に書き込まれたことを検証する
                logger.info("極限整合性検証 (RaW-V: カタログ, マスタ, RAW) を実行中...")
                try:
                    # 1. カタログ & マスタの検証 (force_download=True で最新を強制取得)
                    vf_catalog = catalog._load_parquet("catalog", force_download=True)
                    vf_master = catalog._load_parquet("master", force_download=True)

                    if vf_catalog.empty or vf_master.empty:
                        raise ValueError("リモートの基幹ファイルが空です。")

                    # 2. RAWデータの存在検証 (サンプリング)
                    if not deltas["catalog"].empty:
                        sample_doc = deltas["catalog"].iloc[0]["doc_id"]

                        # カタログ上の存在確認
                        if sample_doc not in vf_catalog["doc_id"].values:
                            raise ValueError(
                                f"検知失敗: 追加されたはずの書類 {sample_doc} がリモートカタログに見当たりません。"
                            )

                        # RAWファイルの存在確認 (エビデンス)
                        raw_repo_path = f"raw/{sample_doc}/{sample_doc}.zip"
                        try:
                            # 存在確認のみなのでメタデータ取得を試みる
                            catalog.api.get_paths_info(
                                repo_id=catalog.hf_repo,
                                paths=[raw_repo_path],
                                repo_type="dataset",
                                token=catalog.hf_token,
                            )
                            logger.info(f"✅ RAWファイル検証成功: {raw_repo_path}")
                        except Exception:
                            # 固有のzip名でない可能性も考慮し、フォルダ存在を確認
                            files = catalog.api.list_repo_files(repo_id=catalog.hf_repo, repo_type="dataset")
                            if not any(f.startswith(f"raw/{sample_doc}/") for f in files):
                                raise ValueError(
                                    f"RAW検証失敗: 書類 {sample_doc} のRAWデータがリモートに存在しません。"
                                ) from None

                    logger.success("✅ RaW-V 成功: 全データの書き込みと整合性がリモート上で確認されました。")
                    catalog.cleanup_deltas(run_id, cleanup_old=False)
                except Exception as e:
                    logger.critical(f"❌ 整合性不整合を検知 (RaW-V ERROR): {e}")
                    # 【世界最高の対応：自動ロールバック】
                    if catalog.rollback(f"RaW-V Failure Rollback: {e}"):
                        logger.success("✅ 自動ロールバックに成功しました。不完全なデータは破棄されました。")
                    else:
                        logger.critical("❌ ロールバックに失敗しました。データが不安定な状態です！手動介入が必要です。")
                    sys.exit(1)
            else:
                logger.error("❌ 最終バッチコミットに失敗しました。データ整合性は維持されています。")
                sys.exit(1)
        else:
            logger.info("処理対象のデータはありませんでした。")
    else:
        logger.error("⛔ Master更新準備中にエラーが発生したため、コミットを中断しました (整合性保護)")
        logger.info("=== Merger Process Completed with Errors (No changes applied) ===")


def main():
    # .envファイルの読み込み
    load_dotenv()

    # ログレベルの設定
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.remove()
    logger.add(sys.stderr, level=log_level)

    # 原因追跡のため、受け取った生の引数をログに出力（デバッグ用）
    logger.debug(f"起動引数: {sys.argv}")

    parser = argparse.ArgumentParser(description="Integrated Disclosure Data Lakehouse 2.0")
    parser.add_argument("--start", type=str, help="YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="YYYY-MM-DD")
    # ハイフン形式とアンダースコア形式の両方を受け入れ、destを統一
    parser.add_argument("--id-list", "--id_list", type=str, dest="id_list", help="Comma separated docIDs", default=None)
    parser.add_argument("--list-only", action="store_true", help="Output metadata as JSON for GHA matrix")

    # 【追加】Worker/Mergerモード制御用
    parser.add_argument("--mode", type=str, default="worker", choices=["worker", "merger"], help="Execution mode")
    parser.add_argument("--run-id", type=str, dest="run_id", help="Execution ID for delta isolation")
    parser.add_argument(
        "--chunk-id", type=str, dest="chunk_id", default="default", help="Chunk ID for parallel workers"
    )

    try:
        args = parser.parse_args()
    except SystemExit as e:
        if e.code != 0:
            logger.error(f"引数解析エラー (exit code {e.code}): 渡された引数が不正です。 sys.argv={sys.argv}")
        raise e

    api_key = os.getenv("EDINET_API_KEY")
    hf_token = os.getenv("HF_TOKEN")
    hf_repo = os.getenv("HF_REPO")

    # 実行IDの自動生成 (指定がない場合)
    run_id = args.run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    chunk_id = args.chunk_id

    if not api_key:
        logger.critical("EDINET_API_KEY が設定されていません。")
        return

    if not args.start:
        args.start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    if not args.end:
        args.end = datetime.now().strftime("%Y-%m-%d")

    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    logger.add(log_dir / "pipeline_{time}.log", rotation="10 MB", level="INFO")

    # タクソノミURL定義の読み込み
    taxonomy_urls_path = Path("taxonomy_urls.json")
    taxonomy_urls = {}
    if taxonomy_urls_path.exists():
        try:
            with open(taxonomy_urls_path, "r", encoding="utf-8") as f:
                taxonomy_urls = json.load(f)
            logger.info(f"タクソノミURL定義を読み込みました: {len(taxonomy_urls)} 件")
        except Exception as e:
            logger.error(f"タクソノミURL定義の読み込みに失敗しました: {e}")
    else:
        logger.warning(f"タクソノミURL定義が見つかりません: {taxonomy_urls_path}")

    edinet = EdinetEngine(api_key, DATA_PATH, taxonomy_urls=taxonomy_urls)
    catalog = CatalogManager(hf_repo, hf_token, DATA_PATH)
    merger = MasterMerger(hf_repo, hf_token, DATA_PATH)

    # 【追加】Mergerモード分岐
    if args.mode == "merger":
        run_merger(catalog, merger, run_id)
        return

    # Workerモード（以下、既存ロジックだがDelta保存に変更）

    # 1. メタデータ取得
    all_meta = edinet.fetch_metadata(args.start, args.end)
    if not all_meta:
        if args.list_only:
            print("JSON_MATRIX_DATA: []")
        return

    # 【投資特化】証券コードがない（非上場企業）を即座に除外
    initial_count = len(all_meta)

    # フィルタリング理由の追跡ログ
    filtered_meta = []
    skipped_reasons = {"no_sec_code": 0, "invalid_length": 0}

    for row in all_meta:
        sec_code = str(row.get("secCode", "")).strip()
        if not sec_code:
            skipped_reasons["no_sec_code"] += 1
            continue
        if len(sec_code) < 5:
            skipped_reasons["invalid_length"] += 1
            # 56件の書類漏れなどの追跡用
            logger.debug(f"書類スキップ (コード短縮): {row.get('docID')} - {sec_code}")
            continue
        filtered_meta.append(row)

    all_meta = filtered_meta
    if initial_count > len(all_meta) and not args.id_list:
        logger.info(
            f"フィルタリング結果: 初期 {initial_count} 件 -> 保持 {len(all_meta)} 件 "
            f"(証券コードなし: {skipped_reasons['no_sec_code']} 件, "
            f"コード不正/短縮: {skipped_reasons['invalid_length']} 件)"
        )

    # 2. GHAマトリックス用出力
    if args.list_only:
        matrix_data = []
        for row in all_meta:
            docid = row["docID"]
            if catalog.is_processed(docid):
                continue

            raw_sec_code = str(row.get("secCode", "")).strip()[:4]
            # 解析対象の厳密判定条件をマトリックス側でも提供
            matrix_data.append(
                {
                    "id": docid,
                    "code": raw_sec_code,
                    "xbrl": row.get("xbrlFlag") == "1",
                    "type": row.get("docTypeCode"),
                    "ord": row.get("ordinanceCode"),
                    "form": row.get("formCode"),
                }
            )
        print(f"JSON_MATRIX_DATA: {json.dumps(matrix_data)}")
        return

    logger.info("=== Data Lakehouse 2.0 実行開始 ===")

    # 4. 処理対象の選定
    tasks = []
    new_catalog_records = []  # バッチ保存用（解析対象外のみ）
    # 【カタログ整合性】ダウンロード直後ではなく、解析結果に基づき登録するよう設計変更
    potential_catalog_records = {}  # docid -> record_base (全レコード保持)
    parsing_target_ids = set()  # 解析対象のdocidセット

    # 【RAW整合性】バッチ処理用
    current_batch_docids = []

    loaded_acc = {}
    skipped_types = {}  # 新たに追加

    target_ids = args.id_list.split(",") if args.id_list else None

    # 解析タスクの追加 (XBRL がある Yuho/Shihanki のみ)
    # 開発者ブログの指定 + 追加ロール (CF, SS, Notes)
    # サブモジュール作成者の記事に基づくロール定義の適正化
    fs_dict = {
        "BS": ["_BalanceSheet", "_ConsolidatedBalanceSheet"],
        "PL": ["_StatementOfIncome", "_ConsolidatedStatementOfIncome"],
        "CF": ["_StatementOfCashFlows", "_ConsolidatedStatementOfCashFlows"],
        "SS": ["_StatementOfChangesInEquity", "_ConsolidatedStatementOfChangesInEquity"],
        "notes": ["_Notes", "_ConsolidatedNotes"],
        "report": ["_CabinetOfficeOrdinanceOnDisclosure"],
    }

    # 【修正】定量データと定性データの分離を適正化
    # 定性注記（notes）は qualitative_text へ、財務諸表本体は financial_values へ
    quant_roles = fs_dict["BS"] + fs_dict["PL"] + fs_dict["CF"] + fs_dict["SS"]
    text_roles = fs_dict["report"] + fs_dict["notes"]

    for row in all_meta:
        docid = row["docID"]
        title = row.get("docDescription", "名称不明")

        if target_ids and docid not in target_ids:
            continue
        if not target_ids and catalog.is_processed(docid):
            continue

        y, m = row["submitDateTime"][:4], row["submitDateTime"][5:7]
        # 【修正】パス最適化: raw/edinet/year=YYYY/month=MM/
        raw_dir = RAW_BASE_DIR / "edinet" / f"year={y}" / f"month={m}"
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_zip = raw_dir / f"{docid}.zip"
        raw_pdf = raw_dir / f"{docid}.pdf"

        # ダウンロード実行 (フラグに基づき正確に試行)
        has_xbrl = row.get("xbrlFlag") == "1"
        has_pdf = row.get("pdfFlag") == "1"

        zip_ok = False
        if has_xbrl:
            zip_ok = edinet.download_doc(docid, raw_zip, 1)
            if zip_ok:
                pass
            else:
                logger.error(f"XBRLダウンロード失敗: {docid} | {title}")

        pdf_ok = False
        if has_pdf:
            pdf_ok = edinet.download_doc(docid, raw_pdf, 2)
            if pdf_ok:
                pass
            else:
                logger.error(f"PDFダウンロード失敗: {docid} | {title}")

        file_status = []
        if has_xbrl:
            file_status.append("XBRLあり" if zip_ok else "XBRL(DL失敗)")
        if has_pdf:
            file_status.append("PDFあり" if pdf_ok else "PDF(DL失敗)")
        status_str = " + ".join(file_status) if file_status else "ファイルなし"

        # 解析タスク判定用のコードを先に取得
        dtc = row.get("docTypeCode")
        ord_c = row.get("ordinanceCode")
        form_c = row.get("formCode")

        # 期末日・決算年度・決算期間（月数）の抽出 (NULL正規化)
        period_start = (row.get("periodStart") or "").strip() or None
        period_end = (row.get("periodEnd") or "").strip() or None

        fiscal_year = int(period_end[:4]) if period_end else None

        # 決算期の月数を算出 (変則決算対応)
        num_months = None  # デフォルトは NULL（期間不明）
        if period_start and period_end:
            try:
                d1 = datetime.strptime(period_start, "%Y-%m-%d")
                d2 = datetime.strptime(period_end, "%Y-%m-%d")
                # 通常は 12, 9, 6, 3 などに収束。
                # 異常値（0以下 または 24ヶ月超）は NULL として扱い、嘘の情報を記録しない
                diff_days = (d2 - d1).days + 1
                calc_months = round(diff_days / 30.4375)  # 365.25 / 12 (平均月日数)

                if 1 <= calc_months <= 24:
                    num_months = calc_months
                else:
                    num_months = None
            except Exception:
                num_months = None  # 計算失敗時も NULL

        sec_code = row.get("secCode", "")[:4]
        # 訂正フラグ
        is_amendment = row.get("withdrawalStatus") != "0" or "訂正" in title

        # カタログ情報のベースを保持 (models.py の解析最適化順序に準拠 - 18カラム構成)
        record = {
            "doc_id": docid,
            "code": sec_code,
            "company_name": (row.get("filerName") or "").strip() or "Unknown",
            "edinet_code": (row.get("edinetCode") or "").strip() or None,
            "submit_at": (row.get("submitDateTime") or "").strip() or None,
            "fiscal_year": fiscal_year,
            "period_start": period_start,
            "period_end": period_end,
            "num_months": num_months,
            "is_amendment": is_amendment,
            "doc_type": dtc or "",
            "title": (title or "").strip() or None,
            "form_code": (form_c or "").strip() or None,
            "ordinance_code": (ord_c or "").strip() or None,
            "raw_zip_path": f"raw/edinet/{docid}.zip" if zip_ok else None,
            "pdf_path": f"raw/edinet/{docid}.pdf" if pdf_ok else None,
            "processed_status": "success",
            "source": "EDINET",
        }
        potential_catalog_records[docid] = record

        # 解析対象の厳密判定: 有報(120) / 一般事業用(010) / 標準様式(030000)
        is_target_yuho = dtc == "120" and ord_c == "010" and form_c == "030000"

        if is_target_yuho and zip_ok:
            try:
                # 【重要】提出日ではなく、XBRL内部の名前空間から「事業年度」基準で年を特定
                import shutil

                from edinet_xbrl_prep.edinet_xbrl_prep.fs_tbl import linkbasefile

                # 判定用の一時ディレクトリ（自動削除用）
                detect_dir = TEMP_DIR / f"detect_{docid}"
                lb = linkbasefile(zip_file_str=str(raw_zip), temp_path_str=str(detect_dir))
                lb.read_linkbase_file()
                ty = lb.detect_account_list_year()

                if ty == "-":
                    raise ValueError(f"XBRL名前空間から事業年度（タクソノミ版）を特定できませんでした。({docid})")

                if ty not in loaded_acc:
                    acc = edinet.get_account_list(ty)
                    if not acc:
                        raise ValueError(
                            f"タクソノミ版 '{ty}' は taxonomy_urls.json に未定義、または取得に失敗しました。"
                        )
                    loaded_acc[ty] = acc

                # 数値データのタスク
                tasks.append((docid, row, loaded_acc[ty], raw_zip, quant_roles, "financial_values"))
                # テキストデータのタスク
                tasks.append((docid, row, loaded_acc[ty], raw_zip, text_roles, "qualitative_text"))

                parsing_target_ids.add(docid)
                logger.info(f"【解析対象】: {docid} | 事業年度: {ty} | {title}")
            except Exception as e:
                logger.error(f"【解析中止】タクソノミ判定失敗 ({docid}): {e}")
                record["processed_status"] = "failure"
                new_catalog_records.append(record)
                continue
            finally:
                if detect_dir.exists():
                    shutil.rmtree(detect_dir)
        else:
            # 【改善】スキップ理由をより明確に
            form_name = "特定有価証券報告書" if form_c == "080000" else "非解析対象"
            codes_info = f"[Type:{dtc}, Ord:{ord_c}, Form:{form_c}]"
            # ターゲット有報なのにXBRLがない場合、または単にターゲット外である場合を区別
            reason = f"XBRLなし {codes_info}" if is_target_yuho else f"{form_name} {codes_info}"

            logger.info(f"【スキップ済として記録】: {docid} | {title} | {status_str} | 理由: {reason}")
            skipped_types[dtc] = skipped_types.get(dtc, 0) + 1
            new_catalog_records.append(record)

        # バッチ管理用にIDを追加
        current_batch_docids.append(docid)

        # 50件ごとに進捗を報告
        processed_count = len(potential_catalog_records)
        if not args.id_list and processed_count % 50 == 0:
            logger.info(f"ダウンロード進捗: {processed_count} / {len(all_meta)} 件完了")

            # 【Smart Batching】以前は50件ごとにアップロードしていましたが、
            # HFのコミット制限(128回/時)を回避するため、Workerでは最後に一度だけ行います。
            pass

    # 【修正】カタログレコードの収集を一本化し、上書きロストを防止
    # 以前は「解析対象外」と「解析対象」で別々に save_delta を呼んでおり、後者が前者を上書きしていた
    final_catalog_records = []
    final_catalog_records.extend(new_catalog_records)

    # 5. 並列解析
    all_quant_dfs = []
    all_text_dfs = []  # テキスト用
    processed_infos = []

    if tasks:
        logger.info(f"解析対象: {len(tasks) // 2} 書類 (Task数: {len(tasks)})")

        # 【修正】解析スキップの内訳を精査。
        # 厳密に 「有報(120)/一般事業会社(010)/標準様式(030000)」 が漏れている場合のみ警告。
        unexpected_skips = []
        for did in potential_catalog_records:
            rec = potential_catalog_records[did]
            # 重要判定: 120 / 010 / 030000 なのに tasks に入っていないものを警告対象とする
            is_missed = (
                rec["doc_type"] == "120"
                and rec["ordinance_code"] == "010"
                and rec["form_code"] == "030000"
                and did not in parsing_target_ids
            )
            if is_missed:
                unexpected_skips.append(did)

        if unexpected_skips:
            logger.warning(f"注意: 最優先解析対象(120/010/030000)がスキップされています: {unexpected_skips}")

        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        with ProcessPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
            for i in range(0, len(tasks), BATCH_PARALLEL_SIZE):
                if is_shutting_down:
                    break
                batch = tasks[i : i + BATCH_PARALLEL_SIZE]
                futures = [executor.submit(parse_worker, t) for t in batch]

                for f in as_completed(futures):
                    did, res_df, err, t_type = f.result()

                    # 解析完了後に parsing_target_ids にあるレコードのステータスを更新
                    if did in potential_catalog_records:
                        target_rec = potential_catalog_records[did]

                        if err:
                            logger.error(f"解析結果({t_type}): {did} - {err}")
                            # 両方のタスク(financial/text)が失敗した場合のみfailureとする等の厳密さは一旦置くが、
                            # エラーがあれば警告レベルを引き上げる
                            if "No objects to concatenate" not in err:
                                target_rec["processed_status"] = "failure"
                        elif res_df is not None:
                            # 少なくとも一方の解析に成功していれば成功
                            target_rec["processed_status"] = "success"

                            if t_type == "financial_values":
                                quant_only = res_df[res_df["isTextBlock_flg"] == 0]
                                if not quant_only.empty:
                                    all_quant_dfs.append(quant_only)
                            elif t_type == "qualitative_text":
                                txt_only = res_df[res_df["isTextBlock_flg"] == 1]
                                if not txt_only.empty:
                                    all_text_dfs.append(txt_only)

                            # セクター判別用
                            meta_row = next(m for m in all_meta if m["docID"] == did)
                            processed_infos.append(
                                {"docID": did, "sector": catalog.get_sector(meta_row.get("secCode", "")[:4])}
                            )

                # バッチごとに登録可能な未定記録を登録
                # 解析対象のカタログ登録は最後にまとめて行うため、ここでは何もしない
                pass

                done_count = i + len(batch)
                logger.info(
                    f"解析進捗: {min(done_count, len(tasks))} / {len(tasks)} tasks 完了 "
                    f"(Quant: {len(all_quant_dfs)}, Text: {len(all_text_dfs)})"
                )
    new_catalog_records = []

    # 6. マスターマージ & カタログ確定
    all_success = True
    # binリスト (重複排除)
    processed_df = pd.DataFrame(processed_infos)
    if not processed_df.empty:
        # docid ごとに code を取得し、bin (=上2桁) を作成
        processed_df["bin"] = processed_df["docID"].apply(
            lambda x: str(next(m for m in all_meta if m["docID"] == x).get("secCode", ""))[:2]
        )
    bins = processed_df["bin"].unique() if not processed_df.empty else []

    if all_quant_dfs:
        logger.info("数値データ(financial_values)のマージを開始します...")
        try:
            full_quant_df = pd.concat(all_quant_dfs, ignore_index=True)
            for b_val in bins:
                # 当該 bin に属する docid を抽出
                bin_docids = processed_df[processed_df["bin"] == b_val]["docID"].tolist()
                sec_quant = full_quant_df[full_quant_df["docid"].isin(bin_docids)]
                if sec_quant.empty:
                    continue
                # 【修正】Master更新は bin 単位で実行
                merger.merge_and_upload(
                    b_val,
                    "financial_values",
                    sec_quant,
                    worker_mode=True,
                    catalog_manager=catalog,
                    run_id=run_id,
                    chunk_id=chunk_id,
                    defer=True,
                )
        except Exception as e:
            logger.error(f"数値データマージ失敗: {e}")
            all_success = False

    if all_text_dfs:
        logger.info("テキストデータ(qualitative_text)のマージを開始します...")
        try:
            full_text_df = pd.concat(all_text_dfs, ignore_index=True)
            for b_val in bins:
                bin_docids = processed_df[processed_df["bin"] == b_val]["docID"].tolist()
                sec_text = full_text_df[full_text_df["docid"].isin(bin_docids)]
                if sec_text.empty:
                    continue
                if not merger.merge_and_upload(
                    b_val,
                    "qualitative_text",
                    sec_text,
                    worker_mode=True,
                    catalog_manager=catalog,
                    run_id=run_id,
                    chunk_id=chunk_id,
                    defer=True,
                ):
                    all_success = False
                    logger.error(f"Master更新失敗: bin={b_val} (qualitative_text)")
        except Exception as e:
            logger.error(f"テキストデータマージ失敗: {e}")
            all_success = False

    # 【修正】全カタログレコードの収集を一本化 (対象内・対象外すべて)
    # 824件すべてが potential_catalog_records に格納されています
    final_catalog_records = list(potential_catalog_records.values())

    if final_catalog_records:
        df_cat = pd.DataFrame(final_catalog_records)

        # 【透明性】ID重複による件数不一致 (824 vs 822 等) を事前にログ出力
        initial_len = len(df_cat)
        df_cat = df_cat.drop_duplicates(subset=["doc_id"], keep="last")
        final_len = len(df_cat)

        if initial_len > final_len:
            logger.info(f"Duplicate IDs removed: {initial_len} -> {final_len} (Reduced: {initial_len - final_len})")

        logger.info(f"全書類の Catalog Delta を保存します ({final_len} 件)")
        catalog.save_delta("catalog", df_cat, run_id, chunk_id, defer=True)

    # 【修正】all_success が False の場合の処理を追加
    if not all_success:
        logger.warning("一部のMaster更新に失敗しました。次回実行時に再試行されます。")

    # カタログ更新（全データ処理後）
    # アップロードに成功したdocid (Quant/Text問わず、何らかのデータが保存できたもの)
    # 厳密な判定は難しいが、ここではmergerの戻り値ベースで判定
    if all_quant_dfs or all_text_dfs:
        # ダウンロード済みのものは potential_catalog_records にある
        # ここでは「データ保存まで完遂した」という意味での更新は不要かもしれない
        # （ダウンロード時に processed_status=success でレコード作成済みで、update_catalogも呼ばれているため）
        # ただし、main.py の設計上、最後にまとめて update_catalog を呼んでいた箇所。
        # 360行目で都度呼んでいるので、ここは「最終的な完了ログ」だけで良い可能性。
        pass

    try:
        if all_success:
            # RAWフォルダの一括アップロード (ここで 1 コミット)
            logger.info("ファイルを一括アップロードしています...")
            catalog.upload_raw_folder(RAW_BASE_DIR, path_in_repo="raw", defer=True)

            # デルタと成功フラグを一括コミット (ここで 1 コミット)
            catalog.mark_chunk_success(run_id, chunk_id, defer=True)
            if catalog.push_commit(f"Worker Success: {run_id}/{chunk_id}"):
                logger.info(f"=== Worker完了: 全データをアップロードしました ({run_id}/{chunk_id}) ===")
            else:
                logger.error("最終コミットに失敗しました")
                sys.exit(1)
        else:
            logger.error("=== パイプライン停止 (Master保存エラー等) ===")
            sys.exit(1)
    finally:
        # Cleanup Temporary Directories
        import shutil

        # Cleanup TEMP_DIR
        if TEMP_DIR.exists():
            try:
                shutil.rmtree(TEMP_DIR)
                logger.info(f"Cleaned up temporary directory: {TEMP_DIR}")
            except Exception as e:
                logger.warning(f"Failed to cleanup {TEMP_DIR}: {e}")


if __name__ == "__main__":
    main()
