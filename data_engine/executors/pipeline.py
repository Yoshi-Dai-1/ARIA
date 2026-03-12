import json
import signal
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from loguru import logger

from data_engine.core.utils import normalize_code, parse_datetime
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
    History, Today, Retry の3期間を重複排除し、最小の API パスで並列マトリックスを生成する。
    """
    # 1. 取得すべきユニーク日付の算出とレンジ連結
    today = datetime.now(ZoneInfo("Asia/Tokyo")).date()
    h_start, h_end = calculate_next_period()
    
    fetch_dates = set()
    if h_start:
        h_start_date = h_start.date() if hasattr(h_start, "date") else h_start
        h_end_date = h_end.date() if hasattr(h_end, "date") else h_end
        
        limit_actual = LIMIT_DATE.date() if hasattr(LIMIT_DATE, "date") else LIMIT_DATE
        r_start = h_start_date - timedelta(days=60)
        if r_start < limit_actual:
            r_start = limit_actual
        
        curr = r_start
        while curr <= h_end_date:
            fetch_dates.add(curr)
            curr += timedelta(days=1)
    fetch_dates.add(today)

    sorted_dates = sorted(list(fetch_dates))
    if not sorted_dates:
        logger.warning("取得対象の日付がありません。")
        return True

    # 連続する日付をレンジ(start, end)に集約して API 実行回数を最小化する
    ranges = []
    if sorted_dates:
        curr_start = sorted_dates[0]
        curr_end = sorted_dates[0]
        for i in range(1, len(sorted_dates)):
            if sorted_dates[i] == curr_end + timedelta(days=1):
                curr_end = sorted_dates[i]
            else:
                ranges.append((curr_start, curr_end))
                curr_start = sorted_dates[i]
                curr_end = sorted_dates[i]
        ranges.append((curr_start, curr_end))

    logger.info(f"Consolidated Discovery 開始: {len(ranges)} pass(es), 全 {len(sorted_dates)} 日分")

    # 判定用エンジンの準備 (WorkerEngine の判定ロジックを流用)
    class MockArgs:
        def __init__(self):
            self.list_only = True
            self.id_list = None
            self.mode = "worker"
    engine = WorkerEngine(MockArgs(), catalog.edinet, catalog, run_id, "discovery")
    
    full_matrix_p_and_t = []
    full_matrix_retry = []
    full_meta_cache = []

    # 【実数カウント】逐次的なカウンタインクリメント
    # 採用: 解析対象, 既処理: 既にDBに存在, コードなし: Scope外(証券コード起因), 取下げ: 取下げ済, 形式不正: 解析非対象
    cnt = {"accept": 0, "processed": 0, "no_code": 0, "withdrawn": 0, "format_err": 0}

    # 2. 最小限のパスでフェッチと判定を実行
    from data_engine.engines.filtering_engine import ProcessVerdict, SkipReason
    for r_start, r_end in ranges:
        s_str = r_start.strftime("%Y-%m-%d")
        e_str = r_end.strftime("%Y-%m-%d")
        meta = catalog.edinet.fetch_metadata(s_str, e_str)
        if not meta:
            continue

        for row in meta:
            doc_id = row.get("docID")
            is_processed = catalog.is_processed(doc_id)
            local_status = catalog.get_status(doc_id)
            verdict, reason, indicators = engine.filtering.get_verdict(row, is_processed, local_status)

            # 【工学的配慮】詳細は DEBUG
            log_prefix = f"[{indicators['doc']}, {indicators['ord']}, {indicators['form']}, XBRL:{indicators['xbrl']}]"
            doc_title = (row.get('docDescription') or 'Unknown').strip()
            log_msg = f"{log_prefix} {doc_id} | {doc_title}"
            logger.debug(f"{log_msg} -> {verdict} ({reason})")

            # 実数カウントロジック (引算不可)
            if verdict == ProcessVerdict.PARSE:
                cnt["accept"] += 1
                full_meta_cache.append(row)
            elif verdict == ProcessVerdict.SKIP_PROCESSED:
                cnt["processed"] += 1
            elif verdict == ProcessVerdict.SKIP_WITHDRAWN:
                cnt["withdrawn"] += 1
            elif verdict == ProcessVerdict.SKIP_OUT_OF_SCOPE:
                no_sec_reasons = [
                    SkipReason.NO_SEC_CODE,
                    SkipReason.INVALID_CODE_LENGTH,
                    SkipReason.HAS_SEC_CODE
                ]
                if reason in no_sec_reasons:
                    cnt["no_code"] += 1
                else:
                    cnt["processed"] += 1  # status_statusStatus 2 等
            elif verdict == ProcessVerdict.SAVE_RAW:
                # 解析非対象の書類すべて
                cnt["format_err"] += 1
                full_meta_cache.append(row)

            # マトリックス用データの生成 (PARSE または SAVE_RAW)
            if verdict in [ProcessVerdict.PARSE, ProcessVerdict.SAVE_RAW]:
                item = {
                    "id": doc_id,
                    "code": normalize_code(str(row.get("secCode", "")).strip(), nationality="JP"),
                    "edinet": row.get("edinetCode"),
                    "xbrl": indicators['xbrl'],
                    "type": indicators['doc'],
                    "ord": indicators['ord'],
                    "form": indicators['form']
                }
                submit_date = parse_datetime(row["submitDateTime"]).date()
                if h_start_date and submit_date < h_start_date:
                    full_matrix_retry.append(item)
                else:
                    full_matrix_p_and_t.append(item)

    # 3. データの保存
    meta_cache_path = catalog.data_path / "meta" / "discovery_metadata.json"
    meta_cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(meta_cache_path, "w", encoding="utf-8") as f:
        json.dump(full_meta_cache, f, ensure_ascii=False, indent=2)

    seen_ids = set()
    unique_p = [x for x in full_matrix_p_and_t if not (x['id'] in seen_ids or seen_ids.add(x['id']))]
    unique_r = [x for x in full_matrix_retry if not (x['id'] in seen_ids or seen_ids.add(x['id']))]

    # GHA matrix 出力
    print(f"JSON_MATRIX_PRIMARY: {json.dumps(unique_p)}")
    print(f"JSON_MATRIX_RETRY: {json.dumps(unique_r)}")

    # 【100% 数学的精度】引き算を一切排除した物理カウントの報告
    total_skip = cnt["processed"] + cnt["no_code"] + cnt["withdrawn"] + cnt["format_err"]
    total_all = cnt["accept"] + total_skip
    detail_str = (
        f"既処理: {cnt['processed']}, 証券コードなし: {cnt['no_code']}, "
        f"取下げ: {cnt['withdrawn']}, 形式不正: {cnt['format_err']}"
    )

    logger.info(
        f"フィルタリング完了: {cnt['accept']}/{total_all} 件を抽出 "
        f"(採用: {cnt['accept']} 件 | 総スキップ: {total_skip} 件 [{detail_str}])"
    )
    logger.info(f"Discovery統合完了: Primary+Today={len(unique_p)}件, Retry={len(unique_r)}件")
    return True
