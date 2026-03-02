"""
Integrated Disclosure Data Lakehouse 2.0
CLI Entrypoint
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from loguru import logger

from data_engine.catalog_manager import CatalogManager
from data_engine.core.config import CONFIG
from data_engine.executors.pipeline import run_merger, run_worker_pipeline


# 共通設定 (SSOT 取得のため定数化不要)
def main():
    # グローバル設定の適用 (LOG_LEVEL 等)
    log_level = os.environ.get("LOG_LEVEL", "INFO")

    # ログレベルの設定
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.remove()
    logger.add(sys.stderr, level=log_level)

    logger.debug(f"起動引数: {sys.argv}")

    parser = argparse.ArgumentParser(description="Integrated Disclosure Data Lakehouse 2.0")
    parser.add_argument("--start", type=str, help="YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="YYYY-MM-DD")
    parser.add_argument("--id-list", "--id_list", type=str, dest="id_list", help="Comma separated docIDs", default=None)
    parser.add_argument("--list-only", action="store_true", help="Output metadata as JSON for GHA matrix")
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

    # 4. Config 経由でのバリデーション (CatalogManager 内部で行われるが、Fail-Fast のため)
    CONFIG.validate_env(production=(args.mode != "merger" and not args.list_only))

    run_id = args.run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    chunk_id = args.chunk_id

    if args.start:
        args.start = args.start.strip()
    if args.end:
        args.end = args.end.strip()

    if not args.start:
        args.start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    if not args.end:
        args.end = datetime.now().strftime("%Y-%m-%d")

    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    logger.add(log_dir / "pipeline_{time}.log", rotation="10 MB", level="INFO")

    # CatalogManager が設定とタクソノミURLを自動ロードする
    catalog = CatalogManager()

    if args.mode == "merger":
        run_merger(catalog, run_id)
    else:
        run_worker_pipeline(args, catalog.edinet, catalog, run_id, chunk_id)


if __name__ == "__main__":
    main()
