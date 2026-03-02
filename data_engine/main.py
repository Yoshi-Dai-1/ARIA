"""
Integrated Disclosure Data Lakehouse 2.0
CLI Entrypoint
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from catalog_manager import CatalogManager
from config import ARIA_SCOPE
from dotenv import load_dotenv
from edinet_engine import EdinetEngine
from loguru import logger
from master_merger import MasterMerger
from pipeline import run_merger, run_worker_pipeline

# 共通設定 (パイプラインへ引き継ぐ定数など)
DATA_PATH = Path("data")


def main():
    # .envファイルの読み込み
    load_dotenv()

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

    api_key = os.getenv("EDINET_API_KEY")
    hf_token = os.getenv("HF_TOKEN")
    hf_repo = os.getenv("HF_REPO")

    run_id = args.run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    chunk_id = args.chunk_id

    if not api_key:
        if args.mode != "merger" and not args.list_only:
            logger.critical("EDINET_API_KEY が設定されていません。")
            return
        else:
            logger.debug(f"EDINET_API_KEY 未設定ですが、{args.mode} モードのため続行します。")

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

    import json

    taxonomy_urls_path = Path(__file__).parent / "taxonomy_urls.json"
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
    catalog = CatalogManager(hf_repo, hf_token, DATA_PATH, scope=ARIA_SCOPE)
    merger = MasterMerger(hf_repo, hf_token, DATA_PATH)

    if args.mode == "merger":
        run_merger(catalog, merger, run_id)
    else:
        run_worker_pipeline(args, edinet, catalog, merger, run_id, chunk_id)


if __name__ == "__main__":
    main()
