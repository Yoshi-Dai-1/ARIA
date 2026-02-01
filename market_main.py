import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from loguru import logger

from catalog_manager import CatalogManager
from market_engine import MarketDataEngine

# 設定
DATA_PATH = Path("data")
TEMP_DIR = DATA_PATH / "temp"


def run_market_pipeline(target_date: str):
    logger.info(f"=== Market Data Pipeline Started (Target Date: {target_date}) ===")

    hf_token = os.getenv("HF_TOKEN")
    hf_repo = os.getenv("HF_REPO")

    if not hf_token or not hf_repo:
        logger.critical("HF_TOKEN / HF_REPO が設定されていません。")
        sys.exit(1)

    # 初期化
    engine = MarketDataEngine(DATA_PATH)
    catalog = CatalogManager(hf_repo, hf_token, DATA_PATH)  # 再利用

    # 1. Stock Master Update (Active/Inactive Logic)
    try:
        new_master = engine.fetch_jpx_master()
        current_master = catalog.master_df.copy()  # Load current

        # Active判定
        active_codes = set(new_master["code"])
        if not current_master.empty:
            if "is_active" not in current_master.columns:
                current_master["is_active"] = True
            current_master["is_active"] = current_master["code"].isin(active_codes)

        new_master["is_active"] = True

        # Merge
        merged_master = pd.concat([current_master, new_master], ignore_index=True).drop_duplicates(
            subset=["code"], keep="last"
        )
        if "rec" in merged_master.columns:
            merged_master.drop(columns=["rec"], inplace=True)

        # Listing Events生成
        # old_listing = catalog.get_listing_history() # 既存履歴
        # events = engine.update_listing_history(current_master, new_master, old_listing)
        # -> current_masterが既に更新後になりうるので、ロジック要調整だが
        #    今回はシンプルにカタログのmaster_df (更新前) を使うのが正しい。
        #    しかし catalog.master_df はプロパティでロード済み。OK。

        old_listing = catalog.get_listing_history()
        listing_events = engine.update_listing_history(catalog.master_df, new_master, old_listing)

        # Save Master
        if catalog._save_and_upload("master", merged_master):
            logger.success(f"Stock Master Updated. Active: {merged_master['is_active'].sum()}")

        # Save History
        if not listing_events.empty:
            merged_hist = pd.concat([old_listing, listing_events], ignore_index=True).drop_duplicates()
            if catalog._save_and_upload("listing", merged_hist):
                logger.success(f"Listing History Updated. Events: {len(listing_events)}")

    except Exception as e:
        logger.error(f"Stock Master更新失敗: {e}")
        # Master更新失敗は致命的なので終了するか検討。
        # ここでは続行をトライ

    # 2. Index Updates (Nikkei225, TOPIX)
    indices = ["Nikkei225", "TOPIX"]

    for index_name in indices:
        logger.info(f"--- Processing {index_name} ---")
        try:
            # A. Fetch Latest Data
            df_new = engine.fetch_index_data(index_name)
            if df_new.empty or len(df_new) < 50:  # 異常検知 (TOPIXなら2000以上あるはず)
                logger.error(f"取得データが少なすぎます ({len(df_new)} rows). スキップします。")
                continue

            # B. Save Snapshot (Year partitioning)
            # path: master/indices/{index}/constituents/year={YYYY}/data_{YYYYMMDD}.parquet
            year = target_date[:4]
            snap_path = (
                f"master/indices/{index_name}/constituents/year={year}/data_{target_date.replace('-', '')}.parquet"
            )
            local_snap = DATA_PATH / f"{index_name}_{target_date}.parquet"
            df_new.to_parquet(local_snap, index=False, compression="zstd")

            catalog.upload_raw(local_snap, snap_path)
            logger.info(f"Snapshot Saved: {snap_path}")

            # C. Update History (Events)
            # 前日のSnapshotを探す
            # target_dateは "YYYY-MM-DD"
            try:
                dt_target = datetime.strptime(target_date, "%Y-%m-%d")
                dt_prev = dt_target - timedelta(days=1)
                prev_date = dt_prev.strftime("%Y-%m-%d")  # "YYYY-MM-DD"
                prev_year = prev_date[:4]
                prev_fname = f"data_{prev_date.replace('-', '')}.parquet"
                prev_path = f"master/indices/{index_name}/constituents/year={prev_year}/{prev_fname}"

                # 前日Snapshotのロード (Catalog経由でダウンロード)
                # CatalogManagerに機能がないためload_rawが必要だが、ここでは簡易的に HF Hub API を使うか
                # あるいは CatalogManager にヘルパーを追加するのがきれい。
                # ここでは engine の責務として一時的にダウンロードする。

                # HfApiは catalog.api が持っている
                local_prev = TEMP_DIR / f"{index_name}_{prev_date}.parquet"

                # ダウンロード試行
                try:
                    # hf_hub_download はパス構造を知っている必要があるが、catalog.upload_raw は単に upload_file。
                    # hf_hub_download(repo_id=..., filename=...) を使う。
                    from huggingface_hub import hf_hub_download

                    hf_hub_download(
                        repo_id=hf_repo,
                        filename=prev_path,
                        token=hf_token,
                        local_dir=str(TEMP_DIR),  # ディレクトリ指定
                        local_dir_use_symlinks=False,
                    )
                    # local_dirを指定すると構造ごと保存される (TEMP_DIR/master/indices/...)
                    # パス解決
                    downloaded_path = TEMP_DIR / prev_path
                    if downloaded_path.exists():
                        df_old = pd.read_parquet(downloaded_path)
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
                        else:
                            df_hist_current = pd.DataFrame()
                    except:
                        df_hist_current = pd.DataFrame()

                    # Merge
                    df_hist_new = pd.concat([df_hist_current, diff_events], ignore_index=True).drop_duplicates()

                    # Save
                    df_hist_new.to_parquet(local_hist, index=False, compression="zstd")
                    catalog.upload_raw(local_hist, hist_path)
                    logger.success(f"History Updated: {index_name} (+{len(diff_events)} events)")
                else:
                    logger.info(f"No changes detected for {index_name}")

            except Exception as e_hist:
                logger.error(f"Failed to update history for {index_name}: {e_hist}")
                # History更新失敗はSnapshot保存を巻き戻すほどではないためログのみ

        except Exception as e:
            logger.error(f"{index_name} 更新失敗: {e}")
            # 個別指数の失敗は他を止めない

    logger.info("=== Market Data Pipeline Completed ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-date", type=str, help="YYYY-MM-DD (Default: Yesterday)")
    args = parser.parse_args()

    # 日付計算
    if args.target_date:
        t_date = args.target_date
    else:
        # デフォルトは昨日
        t_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    run_market_pipeline(t_date)
