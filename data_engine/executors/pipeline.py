import json
import signal
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from loguru import logger

from data_engine.core.utils import normalize_code
from data_engine.engines.merger_engine import MergerEngine
from data_engine.engines.worker_engine import WorkerEngine
from data_engine.executors.backfill_manager import (
    LIMIT_DATE,
    calculate_next_period,
)


# シグナルハンドリング
def signal_handler(sig, frame):
    logger.warning("中断信号を受信しました。シャットダウンしています...")
    # 注意: WorkerEngine内部のプロセスプールに対して停止を波及させる必要がある場合、
    # ここでフラグ制御やプールへの明示的終了指示を行う
    # 現状は SIGINT/SIGTERM により Python ランタイムが停止処理に入る


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def run_worker_pipeline(args, edinet, catalog, run_id, chunk_id):
    """Workerモード (デフォルト): データの取得・解析・保存のパイプラインを実行する

    Orchestrator Responsibility:
    - WorkerEngine のインスタンス化と実行
    """
    engine = WorkerEngine(args, edinet, catalog, run_id, chunk_id)
    return engine.run()


def run_merger(catalog, run_id):
    """Mergerモード: デルタファイルの集約とGlobal更新
    """
    engine = MergerEngine(catalog, run_id)
    return engine.run()


def run_full_discovery(catalog, run_id):
    """
    【工学的主権】ハイブリッド・ディスカバリ (Consolidated Discovery)
    History, Today, Retry の3期間を1回のカタログロードで処理し、並列マトリックスを生成する。
    """
    # 1. 期間の決定
    today_jst = datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d")
    h_start, h_end = calculate_next_period()

    periods = []
    if h_start:
        # History (Primary)
        periods.append(("primary", h_start.strftime("%Y-%m-%d"), h_end.strftime("%Y-%m-%d")))
        # Retry (History の過去60日)
        r_start = h_start - timedelta(days=60)
        if r_start < LIMIT_DATE:
            r_start = LIMIT_DATE
        r_end = h_start - timedelta(days=1)
        if r_start <= r_end:
            periods.append(("retry", r_start.strftime("%Y-%m-%d"), r_end.strftime("%Y-%m-%d")))

    # Today
    periods.append(("today", today_jst, today_jst))

    full_matrix_p_and_t = []
    full_matrix_retry = []
    full_meta_cache = []

    # モック用の引数オブジェクト (WorkerEngine 内で args.list_only 等を参照するため)
    class MockArgs:
        def __init__(self, start, end):
            self.start = start
            self.end = end
            self.list_only = True
            self.id_list = None
            self.mode = "worker"

    # 2. 各期間のフェッチと判定
    for label, start, end in periods:
        logger.info(f"Discoveryフェーズ実行: {label} ({start} ~ {end})")
        m_args = MockArgs(start, end)
        engine = WorkerEngine(m_args, catalog.edinet, catalog, run_id, f"disc-{label}")

        meta = catalog.edinet.fetch_metadata(start, end)
        if not meta:
            continue

        period_filtered = []
        period_matrix = []

        for row in meta:
            doc_id = row.get("docID")
            is_processed = catalog.is_processed(doc_id)
            local_status = catalog.get_status(doc_id)
            verdict, _, _ = engine.filtering.get_verdict(row, is_processed, local_status)

            if verdict in ["parse", "save_meta"]:
                period_filtered.append(row)
                period_matrix.append({
                    "id": doc_id,
                    "code": normalize_code(str(row.get("secCode", "")).strip(), nationality="JP"),
                    "edinet": row.get("edinetCode"),
                    "xbrl": row.get("xbrlFlag") == "1",
                    "type": row.get("docTypeCode"),
                    "ord": row.get("ordinanceCode"),
                    "form": row.get("formCode")
                })

        full_meta_cache.extend(period_filtered)
        if label == "retry":
            full_matrix_retry.extend(period_matrix)
        else:
            full_matrix_p_and_t.extend(period_matrix)

    # 3. データの保存
    meta_cache_path = catalog.data_path / "meta" / "discovery_metadata.json"
    meta_cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(meta_cache_path, "w", encoding="utf-8") as f:
        json.dump(full_meta_cache, f, ensure_ascii=False, indent=2)

    # 重複排除 (Primary + Today)
    seen_ids = set()
    unique_primary_and_today = [
        x for x in full_matrix_p_and_t if not (x['id'] in seen_ids or seen_ids.add(x['id']))
    ]

    # GHA matrix 出力
    print(f"JSON_MATRIX_PRIMARY: {json.dumps(unique_primary_and_today)}")
    print(f"JSON_MATRIX_RETRY: {json.dumps(full_matrix_retry)}")

    logger.info(f"Discovery統合完了: Primary+Today={len(unique_primary_and_today)}件, Retry={len(full_matrix_retry)}件")
    return True
