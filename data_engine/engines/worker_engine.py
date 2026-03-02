import json
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
from loguru import logger

from data_engine.core.config import ARIA_SCOPE, CONFIG, HF_WARNING_THRESHOLD, RAW_DIR, TEMP_DIR
from data_engine.core.utils import normalize_code
from data_engine.edinet_xbrl_prep.fs_tbl import get_fs_tbl

# 設定 (SSOT から取得)
PARALLEL_WORKERS = CONFIG.PARALLEL_WORKERS
BATCH_PARALLEL_SIZE = CONFIG.BATCH_PARALLEL_SIZE
RAW_BASE_DIR = RAW_DIR


def parse_datetime(dt_str: str):
    """EDINET の submitDateTime (YYYY-MM-DD HH:MM[:SS]) を堅牢にパースする"""
    if not dt_str or not isinstance(dt_str, str):
        return datetime.now()
    try:
        if len(dt_str) > 16:
            return datetime.strptime(dt_str[:19], "%Y-%m-%d %H:%M:%S")
        return datetime.strptime(dt_str[:16], "%Y-%m-%d %H:%M")
    except Exception:
        return datetime.now()


def parse_worker(args):
    """並列処理用ワーカー関数"""
    docid, row, acc_obj, raw_zip, role_kws, task_type = args
    extract_dir = TEMP_DIR / f"extract_{docid}_{task_type}"
    try:
        if acc_obj is None:
            return docid, None, "Account list not loaded"

        logger.debug(f"解析開始: {docid} (Path: {raw_zip})")
        (extract_dir / "XBRL" / "PublicDoc").mkdir(parents=True, exist_ok=True)

        from zipfile import ZipFile

        with ZipFile(str(raw_zip)) as zf:
            for member in zf.namelist():
                if "PublicDoc" in member or "AuditDoc" in member:
                    zf.extract(member, extract_dir)

        df = get_fs_tbl(
            account_list_common_obj=acc_obj,
            docid=docid,
            zip_file_str=str(raw_zip),
            temp_path_str=str(extract_dir),
            role_keyward_list=role_kws,
        )

        if df is not None and not df.empty:
            df["docid"] = docid
            sec_code_meta = row.get("secCode")
            if sec_code_meta and str(sec_code_meta).strip():
                df["code"] = normalize_code(sec_code_meta)
            else:
                df["code"] = None

            df["submitDateTime"] = row.get("submitDateTime", "")
            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = df[col].astype(str)
            logger.debug(f"解析成功: {docid} ({task_type}) | 抽出レコード数: {len(df)}")

            accounting_std = None
            log_path = extract_dir / "XBRL" / "PublicDoc" / "log_dict.json"
            if log_path.exists():
                try:
                    with open(log_path, "r", encoding="utf-8") as f:
                        log_data = json.load(f)
                    accounting_std = log_data.get("AccountingStandardsDEI")
                except Exception as e:
                    logger.warning(f"log_dict.json の読み込み失敗 ({docid}): {e}")

            return docid, df, None, (task_type, accounting_std)

        msg = "No objects to concatenate" if (df is None or df.empty) else "Empty Results"
        return docid, None, msg, (task_type, None)

    except Exception as e:
        import traceback

        err_detail = traceback.format_exc()
        logger.error(f"解析例外: {docid} ({task_type})\n{err_detail}")
        return docid, None, f"{str(e)}", (task_type, None)
    finally:
        if extract_dir.exists():
            import shutil

            shutil.rmtree(extract_dir)


class WorkerEngine:
    def __init__(self, args, edinet, catalog, run_id, chunk_id):
        self.args = args
        self.edinet = edinet
        self.catalog = catalog
        self.merger = catalog.merger
        self.run_id = run_id
        self.chunk_id = chunk_id
        self.is_shutting_down = False

    def run(self):
        """Workerモード (デフォルト): データの取得・解析・保存のパイプラインを実行する"""
        logger.info("=== Worker Pipeline Started ===")

        # 1. 増分同期のためのチェックポイント取得
        last_ope_time = None
        if not self.catalog.catalog_df.empty and "ope_date_time" in self.catalog.catalog_df.columns:
            # カタログ内の最新の操作日時を取得
            max_ope = self.catalog.catalog_df["ope_date_time"].max()
            if not pd.isna(max_ope) and str(max_ope).strip() != "":
                try:
                    # 【工学的主権】APIの反映遅延（Visibility Lag）に備え、1時間のルックバックバッファを適用する
                    # 14:00に取得した際、13:55の書類が見えていなかったとしても、次回の1時間戻した検索で確実に捉える。
                    # 重複は CatalogManager.update_catalog の drop_duplicates で排除されるため安全。
                    dt_ope = datetime.strptime(max_ope, "%H:%M:%S")
                    dt_buffer = dt_ope - pd.Timedelta(hours=1)
                    # 00:00:00 を下回らないように調整
                    if dt_buffer.day < dt_ope.day:  # 日を跨いだ場合
                        last_ope_time = "00:00:00"
                    else:
                        last_ope_time = dt_buffer.strftime("%H:%M:%S")
                    logger.info(f"増分同期チェックポイント: {max_ope} -> {last_ope_time} (1h Buffer適用)")
                except Exception as e:
                    logger.warning(f"ope_date_time の計算に失敗しました: {e}")
                    last_ope_time = None

        all_meta = self.edinet.fetch_metadata(self.args.start, self.args.end, ope_date_time=last_ope_time)
        if not all_meta:
            if self.args.list_only:
                print("JSON_MATRIX_DATA: []")
            return

        initial_count = len(all_meta)
        filtered_meta = []
        skipped_reasons = {"no_sec_code": 0, "invalid_length": 0, "has_sec_code": 0}

        for row in all_meta:
            raw_code = row.get("secCode")
            sec_code = "" if raw_code is None else str(raw_code).strip()
            if sec_code.lower() in ["none", "nan", "null"]:
                sec_code = ""

            if ARIA_SCOPE == "Listed":
                if not sec_code:
                    skipped_reasons["no_sec_code"] += 1
                    continue
                if len(sec_code) < 4:
                    skipped_reasons["invalid_length"] += 1
                    logger.debug(f"書類スキップ (コード短縮): {row.get('docID')} - {sec_code}")
                    continue
                filtered_meta.append(row)
            elif ARIA_SCOPE == "Unlisted":
                if sec_code and len(sec_code) >= 4:
                    skipped_reasons["has_sec_code"] += 1
                    continue
                filtered_meta.append(row)
            elif ARIA_SCOPE == "All":
                filtered_meta.append(row)
            else:
                logger.warning(f"Unknown ARIA_SCOPE: {ARIA_SCOPE}. Falling back to skip.")
                continue

        all_meta = filtered_meta
        if not self.args.id_list:
            logger.info(
                f"最終処理対象書類数: {len(all_meta)} 件 "
                f"(全 {initial_count} 件中 | 証券コードなし: {skipped_reasons.get('no_sec_code', 0)} 件, "
                f"コード不正: {skipped_reasons.get('invalid_length', 0)} 件, "
                f"上場企業コードあり: {skipped_reasons.get('has_sec_code', 0)} 件)"
            )

        meta_cache_path = Path("data/meta/discovery_metadata.json")
        if self.args.list_only:
            meta_cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(meta_cache_path, "w", encoding="utf-8") as f:
                json.dump(all_meta, f, ensure_ascii=False, indent=2)
            logger.info(f"Discoveryメタデータを保存しました: {meta_cache_path}")
        elif self.args.mode == "worker" and meta_cache_path.exists():
            try:
                with open(meta_cache_path, "r", encoding="utf-8") as f:
                    all_meta = json.load(f)
                logger.info("Discovery時のメタデータキャッシュを利用してDriftを防止します。")
            except Exception as e:
                logger.warning(f"メタデータキャッシュの読み込みに失敗しました: {e}")

        if self.args.list_only:
            matrix_data = []
            for row in all_meta:
                docid = row["docID"]
                if self.catalog.is_processed(docid):
                    api_retracted = row.get("withdrawalStatus") == "1"
                    local_status = self.catalog.get_status(docid)
                    if not (api_retracted and local_status != "retracted"):
                        continue

                raw_sec_code = normalize_code(str(row.get("secCode", "")).strip())
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
        tasks = []
        potential_catalog_records = {}
        parsing_target_ids = set()
        found_target_ids = set()

        fs_dict = {
            "BS": ["_BalanceSheet", "_ConsolidatedBalanceSheet"],
            "PL": ["_StatementOfIncome", "_ConsolidatedStatementOfIncome"],
            "CF": ["_StatementOfCashFlows", "_ConsolidatedStatementOfCashFlows"],
            "SS": ["_StatementOfChangesInEquity", "_ConsolidatedStatementOfChangesInEquity"],
            "notes": ["_Notes", "_ConsolidatedNotes"],
            "report": ["_CabinetOfficeOrdinanceOnDisclosure"],
        }
        quant_roles = fs_dict["BS"] + fs_dict["PL"] + fs_dict["CF"] + fs_dict["SS"]
        text_roles = fs_dict["report"] + fs_dict["notes"]

        loaded_acc = {}
        target_ids = self.args.id_list.split(",") if self.args.id_list else None

        for row in all_meta:
            doc_id = row.get("docID")
            edinet_code = row.get("edinetCode")
            title = row.get("docDescription", "名称不明")

            is_listed_meta = False
            if edinet_code:
                m_rec = self.catalog.master_df[self.catalog.master_df["edinet_code"] == edinet_code]
                if not m_rec.empty:
                    is_listed_meta = bool(m_rec.iloc[0].get("is_listed_edinet", False))

            if is_listed_meta != (ARIA_SCOPE == "Listed"):
                continue

            if target_ids and doc_id not in target_ids:
                continue

            if target_ids:
                found_target_ids.add(doc_id)

            if not getattr(self.args, "force_refresh", False) and self.catalog.is_processed(doc_id):
                continue

            submit_date = parse_datetime(row["submitDateTime"])
            save_dir = (
                RAW_BASE_DIR
                / "edinet"
                / f"year={submit_date.year}"
                / f"month={submit_date.month:02d}"
                / f"day={submit_date.day:02d}"
            )
            zip_dir = save_dir / "zip"
            pdf_dir = save_dir / "pdf"
            raw_zip = zip_dir / f"{doc_id}.zip"
            raw_pdf = pdf_dir / f"{doc_id}.pdf"
            zip_dir.mkdir(parents=True, exist_ok=True)
            pdf_dir.mkdir(parents=True, exist_ok=True)

            has_xbrl = row.get("xbrlFlag") == "1"
            has_pdf = row.get("pdfFlag") == "1"

            zip_ok = False
            if has_xbrl:
                zip_ok = self.edinet.download_doc(doc_id, raw_zip, 1)

            pdf_ok = False
            if has_pdf:
                pdf_ok = self.edinet.download_doc(doc_id, raw_pdf, 2)

            dtc = row.get("docTypeCode")
            ord_c = row.get("ordinanceCode")
            form_c = row.get("formCode")
            period_start = (row.get("periodStart") or "").strip() or None
            period_end = (row.get("periodEnd") or "").strip() or None
            fiscal_year = int(period_end[:4]) if period_end else None

            num_months = None
            if period_start and period_end:
                try:
                    d1 = datetime.strptime(period_start, "%Y-%m-%d")
                    d2 = datetime.strptime(period_end, "%Y-%m-%d")
                    diff_days = (d2 - d1).days + 1
                    calc_months = round(diff_days / 30.4375)
                    if 1 <= calc_months <= 24:
                        num_months = calc_months
                except Exception:
                    pass

            sec_code = normalize_code(row.get("secCode", ""))
            parent_id = row.get("parentDocID")
            is_amendment = parent_id is not None or str(dtc).endswith("1") or "訂正" in (title or "")
            w_status = row.get("withdrawalStatus")
            final_status = "retracted" if w_status == "1" else "pending"

            rel_zip_path = str(raw_zip.relative_to(RAW_BASE_DIR.parent)) if zip_ok else None
            rel_pdf_path = str(raw_pdf.relative_to(RAW_BASE_DIR.parent)) if pdf_ok else None

            record = {
                "doc_id": doc_id,
                "jcn": (row.get("JCN") or "").strip() or None,
                "code": sec_code,
                "company_name": (row.get("filerName") or "").strip() or "Unknown",
                "edinet_code": (row.get("edinetCode") or "").strip() or None,
                "issuer_edinet_code": (row.get("issuerEdinetCode") or "").strip() or None,
                "subject_edinet_code": (row.get("subjectEdinetCode") or "").strip() or None,
                "subsidiary_edinet_code": (row.get("subsidiaryEdinetCode") or "").strip() or None,
                "fund_code": (row.get("fundCode") or "").strip() or None,
                "submit_at": row.get("submitDateTime"),
                "fiscal_year": fiscal_year,
                "period_start": period_start,
                "period_end": period_end,
                "num_months": num_months,
                "accounting_standard": None,
                "doc_type": dtc or "",
                "title": (title or "").strip() or None,
                "form_code": (form_c or "").strip() or None,
                "ordinance_code": (ord_c or "").strip() or None,
                "is_amendment": is_amendment,
                "parent_doc_id": (parent_id or "").strip() or None,
                "withdrawal_status": (w_status or "").strip() or None,
                "doc_info_edit_status": (row.get("docInfoEditStatus") or "").strip() or None,
                "disclosure_status": (row.get("disclosureStatus") or "").strip() or None,
                "current_report_reason": (row.get("currentReportReason") or "").strip() or None,
                "raw_zip_path": rel_zip_path,
                "pdf_path": rel_pdf_path,
                "processed_status": final_status,
                "source": "EDINET",
                "ope_date_time": (row.get("opeDateTime") or "").strip() or None,
            }
            potential_catalog_records[doc_id] = record

            is_target_yuho = dtc == "120" and ord_c == "010" and form_c == "030000"
            if is_target_yuho and zip_ok:
                try:
                    import shutil

                    from data_engine.edinet_xbrl_prep.fs_tbl import linkbasefile

                    detect_dir = TEMP_DIR / f"detect_{doc_id}"
                    lb = linkbasefile(zip_file_str=str(raw_zip), temp_path_str=str(detect_dir))
                    lb.read_linkbase_file()
                    ty = lb.detect_account_list_year()

                    if ty == "-":
                        raise ValueError(f"Taxonomy year not identified for {doc_id}")

                    if ty not in loaded_acc:
                        acc = self.edinet.get_account_list(ty)
                        if not acc:
                            raise ValueError(f"Taxonomy version '{ty}' not found")
                        loaded_acc[ty] = acc

                    tasks.append((doc_id, row, loaded_acc[ty], raw_zip, quant_roles, "financial_values"))
                    tasks.append((doc_id, row, loaded_acc[ty], raw_zip, text_roles, "qualitative_text"))
                    parsing_target_ids.add(doc_id)
                    logger.info(f"【解析対象】: {doc_id} | 事業年度: {ty} | {title}")
                except Exception as e:
                    logger.error(f"【解析中止】タクソノミ判定失敗 ({doc_id}): {e}")
                    record["processed_status"] = "failure"
                    continue
                finally:
                    if detect_dir.exists():
                        shutil.rmtree(detect_dir)
            else:
                if not is_target_yuho:
                    record["processed_status"] = "success"
                elif is_target_yuho and not zip_ok:
                    record["processed_status"] = "failure"

        # HF 警告
        checked_dirs = set()
        for row in all_meta:
            sd = parse_datetime(row.get("submitDateTime", ""))
            day_dir = RAW_BASE_DIR / "edinet" / f"year={sd.year}" / f"month={sd.month:02d}" / f"day={sd.day:02d}"
            if day_dir not in checked_dirs and day_dir.exists():
                checked_dirs.add(day_dir)
                file_count = sum(1 for _ in day_dir.iterdir())
                if file_count > HF_WARNING_THRESHOLD:
                    logger.warning(f"⚠️ HFフォルダファイル数警告: {day_dir.name} に {file_count} ファイル")

        if target_ids:
            missing_ids = set(target_ids) - found_target_ids
            if missing_ids:
                logger.critical(f"Drift detected: Missing IDs {list(missing_ids)}")

        all_quant_dfs = []
        all_text_dfs = []
        processed_infos = []

        if tasks:
            logger.info(f"解析対象: {len(tasks) // 2} 書類 (Task数: {len(tasks)})")
            TEMP_DIR.mkdir(parents=True, exist_ok=True)
            with ProcessPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
                for i in range(0, len(tasks), BATCH_PARALLEL_SIZE):
                    if self.is_shutting_down:
                        break
                    batch = tasks[i : i + BATCH_PARALLEL_SIZE]
                    futures = [executor.submit(parse_worker, t) for t in batch]

                    for f in as_completed(futures):
                        did, res_df, err, worker_meta = f.result()
                        t_type, accounting_std = worker_meta

                        if did in potential_catalog_records:
                            target_rec = potential_catalog_records[did]
                            if err:
                                logger.error(f"解析失敗 ({t_type}): {did} - {err}")
                                if "No objects to concatenate" not in err:
                                    target_rec["processed_status"] = "failure"
                            elif res_df is not None:
                                if target_rec.get("processed_status") != "failure":
                                    target_rec["processed_status"] = "success"

                                if t_type == "financial_values":
                                    quant_only = res_df[res_df["isTextBlock_flg"] == 0]
                                    if not quant_only.empty:
                                        all_quant_dfs.append(quant_only)
                                elif t_type == "qualitative_text":
                                    txt_only = res_df[res_df["isTextBlock_flg"] == 1]
                                    if not txt_only.empty:
                                        all_text_dfs.append(txt_only)

                                if accounting_std:
                                    target_rec["accounting_standard"] = str(accounting_std)

                                meta_row = next(m for m in all_meta if m["docID"] == did)
                                processed_infos.append(
                                    {
                                        "docID": did,
                                        "sector": self.catalog.get_sector(normalize_code(meta_row.get("secCode", ""))),
                                    }
                                )
                    logger.info(f"解析進捗: {min(i + BATCH_PARALLEL_SIZE, len(tasks))} / {len(tasks)} tasks 完了")

        all_success = True
        processed_df = pd.DataFrame(processed_infos)
        if not processed_df.empty:
            # 【極限統一】MasterMerger のロジックを用いて不変の分散キー(bin)を決定する
            def _get_row_bin(docid):
                meta = next(m for m in all_meta if m["docID"] == docid)
                # MasterMerger が期待するキー名 (小文字/スネークケース) にマッピング
                bridge_row = {"jcn": meta.get("JCN"), "edinet_code": meta.get("edinetCode")}
                return self.merger.get_bin_id(bridge_row)

            processed_df["bin"] = processed_df["docID"].apply(_get_row_bin)
            bins = processed_df["bin"].unique()

            if all_quant_dfs:
                try:
                    full_quant_df = pd.concat(all_quant_dfs, ignore_index=True)
                    for b_val in bins:
                        bin_docids = processed_df[processed_df["bin"] == b_val]["docID"].tolist()
                        sec_quant = full_quant_df[full_quant_df["docid"].isin(bin_docids)]
                        if not sec_quant.empty:
                            self.merger.merge_and_upload(
                                b_val,
                                "financial_values",
                                sec_quant,
                                worker_mode=True,
                                catalog_manager=self.catalog,
                                run_id=self.run_id,
                                chunk_id=self.chunk_id,
                                defer=True,
                            )
                except Exception as e:
                    logger.error(f"Quant merge failed: {e}")
                    all_success = False

            if all_text_dfs:
                try:
                    full_text_df = pd.concat(all_text_dfs, ignore_index=True)
                    for b_val in bins:
                        bin_docids = processed_df[processed_df["bin"] == b_val]["docID"].tolist()
                        sec_text = full_text_df[full_text_df["docid"].isin(bin_docids)]
                        if not sec_text.empty:
                            self.merger.merge_and_upload(
                                b_val,
                                "qualitative_text",
                                sec_text,
                                worker_mode=True,
                                catalog_manager=self.catalog,
                                run_id=self.run_id,
                                chunk_id=self.chunk_id,
                                defer=True,
                            )
                except Exception as e:
                    logger.error(f"Text merge failed: {e}")
                    all_success = False

        final_catalog_records = list(potential_catalog_records.values())
        if final_catalog_records:
            df_cat = pd.DataFrame(final_catalog_records).drop_duplicates(subset=["doc_id"], keep="last")
            self.catalog.save_delta("catalog", df_cat, self.run_id, self.chunk_id, defer=True, local_only=True)

        if all_success:
            self.catalog.mark_chunk_success(self.run_id, self.chunk_id, defer=True, local_only=True)
            logger.success(f"=== Worker完了: {self.run_id}/{self.chunk_id} ===")
            return True
        else:
            logger.error("=== Worker停止 (エラーあり) ===")
            sys.exit(1)
            return False
