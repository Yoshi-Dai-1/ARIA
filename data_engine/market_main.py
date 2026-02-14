import os

# CI環境でのプログレスバーを無効化 (ログの肥大化・視認性低下を防ぐため全ライブラリ前に設定)
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TQDM_DISABLE"] = "1"

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import tqdm as tqdm_mod
from catalog_manager import CatalogManager
from loguru import logger
from market_engine import MarketDataEngine
from network_utils import patch_all_networking
from tqdm import tqdm


def no_op_tqdm(*args, **kwargs):
    kwargs.update({"disable": True})
    return tqdm(*args, **kwargs)


tqdm_mod.tqdm = no_op_tqdm

# 設定
DATA_PATH = Path("data").resolve()
TEMP_DIR = DATA_PATH / "temp"


def run_market_pipeline(target_date: str, mode: str = "all"):
    logger.info(f"=== Market Data Pipeline Started (Target Date: {target_date}) ===")

    # 全体的な通信の堅牢化を適用
    patch_all_networking()

    hf_token = os.getenv("HF_TOKEN")
    hf_repo = os.getenv("HF_REPO")

    if not hf_token or not hf_repo:
        logger.critical("HF_TOKEN / HF_REPO が設定されていません。")
        sys.exit(1)

    # 初期化
    DATA_PATH.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    engine = MarketDataEngine(DATA_PATH)
    catalog = CatalogManager(hf_repo, hf_token, DATA_PATH)  # 再利用

    import shutil

    try:
        # 1. Stock Master Update (Active/Inactive Logic)
        if mode in ["all", "master"]:
            try:
                new_master = engine.fetch_jpx_master()
                current_master = catalog.master_df.copy()

                # 三値論理（Active/Inactive/Unknown）の適用判定
                active_codes = set(new_master["code"])
                # 過去にJPXに存在した証拠（日付が NULL）があるコードを特定
                jpx_history_codes = set(current_master[current_master["last_submitted_at"].isna()]["code"])

                # 更新用データの構築
                # A. 現役 (True)
                active_updates = new_master.copy()
                active_updates["is_active"] = True
                active_updates["last_submitted_at"] = None

                # B. 廃止 (False) : 過去にいたが今はいない
                delisted_codes = jpx_history_codes - active_codes
                delisted_updates = current_master[current_master["code"].isin(delisted_codes)].copy()
                delisted_updates["is_active"] = False
                delisted_updates["last_submitted_at"] = None

                incoming_updates = pd.concat([active_updates, delisted_updates], ignore_index=True)

                # 【究極の統合】カタログマネージャの属性承継ロジックに委ねる
                # これにより、最新の社名(EDINET)に対し、JPXの属性が正しく引き継がれる。
                if catalog.update_stocks_master(incoming_updates):
                    logger.success("Market Master synchronized and attributes reconciled.")
                else:
                    raise ValueError("Failed to reconcile market master updates.")

                # Listing Events生成
                old_listing = catalog.get_listing_history()
                listing_events = engine.update_listing_history(catalog.master_df, new_master, old_listing)

                # Save History (Deferred)
                if not listing_events.empty:
                    merged_hist = pd.concat([old_listing, listing_events], ignore_index=True).drop_duplicates()
                    catalog._save_and_upload("listing", merged_hist, defer=True)
                    logger.info("Listing History updated in buffer.")
                elif old_listing.empty:
                    # 初回初期化: 空のDFをアップロード
                    catalog._save_and_upload("listing", old_listing, defer=True)
                    logger.info("Listing History initialized in buffer.")

            except Exception as e:
                logger.critical(f"Stock Master更新失敗 (Fatal): {e}")
                raise  # 即座に停止し、部分的なコミットを防止する

        # 2. Index Updates (Nikkei225, TOPIX, etc.)
        if mode in ["all", "indices"]:
            # 動的に全戦略を取得
            indices = list(engine.strategies.keys())

            for index_name in indices:
                logger.info(f"--- Processing {index_name} ---")
                try:
                    # A. Fetch Latest Data
                    df_new = engine.fetch_index_data(index_name)
                    if df_new.empty or len(df_new) < 50:  # 異常検知 (TOPIXなら2000以上あるはず)
                        logger.error(f"取得データが少なすぎます ({len(df_new)} rows). スキップします。")
                        continue

                    # Clean df_new just in case
                    if "rec" in df_new.columns:
                        df_new.drop(columns=["rec"], inplace=True)

                    # B. Save Snapshot (Year partitioning)
                    # path: master/indices/{index}/constituents/year={YYYY}/data_{YYYYMMDD}.parquet
                    year = target_date[:4]
                    snap_path = (
                        f"master/indices/{index_name}/constituents/"
                        f"year={year}/data_{target_date.replace('-', '')}.parquet"
                    )
                    local_snap = DATA_PATH / f"{index_name}_{target_date}.parquet"
                    df_new.to_parquet(local_snap, index=False, compression="zstd")

                    catalog.upload_raw(local_snap, snap_path, defer=True)
                    logger.info(f"Snapshot staged: {snap_path}")

                    # C. Update History (Events)
                    # 前日のSnapshotを探す
                    try:
                        dt_target = datetime.strptime(target_date, "%Y-%m-%d")
                        dt_prev = dt_target - timedelta(days=1)
                        prev_date = dt_prev.strftime("%Y-%m-%d")  # "YYYY-MM-DD"
                        prev_year = prev_date[:4]
                        prev_fname = f"data_{prev_date.replace('-', '')}.parquet"
                        prev_path = f"master/indices/{index_name}/constituents/year={prev_year}/{prev_fname}"

                        # 前日Snapshotのロード (Catalog経由でダウンロード)
                        try:
                            from huggingface_hub import hf_hub_download

                            hf_hub_download(
                                repo_id=hf_repo,
                                filename=prev_path,
                                token=hf_token,
                                local_dir=str(TEMP_DIR),  # ディレクトリ指定
                                local_dir_use_symlinks=False,
                            )
                            downloaded_path = TEMP_DIR / prev_path
                            if downloaded_path.exists():
                                df_old = pd.read_parquet(downloaded_path)
                                # 【重要】既存データからの汚染除去
                                if "rec" in df_old.columns:
                                    df_old.drop(columns=["rec"], inplace=True)
                                logger.info(f"Loaded Previous Snapshot: {prev_date}")
                            else:
                                logger.warning(f"Previous Snapshot file not found at local: {downloaded_path}")
                                df_old = pd.DataFrame(columns=["code", "weight"])

                        except Exception as e_dl:
                            logger.warning(f"Previous Snapshot not found in Repo ({prev_date}): {e_dl}")
                            df_old = pd.DataFrame(columns=["code", "weight"])

                        # Diff生成
                        diff_events = engine.generate_index_diff(index_name, df_old, df_new, target_date)

                        if not diff_events.empty:
                            # Historyファイルのロードと追記
                            # master/indices/{index_name}/history.parquet
                            hist_path = f"master/indices/{index_name}/history.parquet"
                            local_hist = DATA_PATH / f"{index_name}_history.parquet"

                            # 既存History取得
                            try:
                                hf_hub_download(
                                    repo_id=hf_repo,
                                    filename=hist_path,
                                    token=hf_token,
                                    local_dir=str(TEMP_DIR),
                                    local_dir_use_symlinks=False,
                                )
                                dl_hist_path = TEMP_DIR / hist_path
                                if dl_hist_path.exists():
                                    df_hist_current = pd.read_parquet(dl_hist_path)
                                    # 【重要】既存データからの汚染除去
                                    if "rec" in df_hist_current.columns:
                                        df_hist_current.drop(columns=["rec"], inplace=True)
                                else:
                                    df_hist_current = pd.DataFrame()
                            except Exception:
                                df_hist_current = pd.DataFrame()

                            # Merge
                            df_hist_new = pd.concat([df_hist_current, diff_events], ignore_index=True).drop_duplicates()
                            # 【重要】保存前最終チェック
                            if "rec" in df_hist_new.columns:
                                df_hist_new.drop(columns=["rec"], inplace=True)

                            # Save (Deferred)
                            df_hist_new.to_parquet(local_hist, index=False, compression="zstd")
                            catalog.upload_raw(local_hist, hist_path, defer=True)
                            logger.info(f"History staged: {index_name}")
                        elif df_hist_current.empty:
                            # 初回実行時: 空でもファイルを作成してアップロード
                            local_hist = DATA_PATH / f"{index_name}_history.parquet"
                            df_hist_current.to_parquet(local_hist, index=False, compression="zstd")
                            catalog.upload_raw(local_hist, hist_path, defer=True)
                            logger.info(f"History initialized: {index_name}")
                        else:
                            logger.info(f"No changes detected for {index_name}")

                    except Exception as e_hist:
                        logger.error(f"Failed to update history for {index_name}: {e_hist}")

                except Exception as e:
                    logger.error(f"{index_name} 更新失敗: {e}")

        # Final Push
        if catalog.push_commit(f"Market Data Update: {target_date}"):
            logger.success("=== Market Data Pipeline Completed ===")
        else:
            logger.error("=== Market Data Pipeline Failed (Push Error) ===")
            sys.exit(1)

    finally:
        # 一時ファイルの削除
        if TEMP_DIR.exists():
            try:
                shutil.rmtree(TEMP_DIR)
                logger.info("Temporary files cleaned up.")
            except Exception as e:
                logger.warning(f"Failed to cleanup temp dir: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-date", type=str, help="YYYY-MM-DD (Default: Yesterday)")
    parser.add_argument("--mode", type=str, choices=["all", "master", "indices"], default="all", help="Execution mode")
    args = parser.parse_args()

    # 日付計算
    if args.target_date:
        t_date = args.target_date
    else:
        # デフォルトは昨日
        t_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    run_market_pipeline(t_date, args.mode)
