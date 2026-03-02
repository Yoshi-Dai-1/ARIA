import signal

from loguru import logger

# モジュールのインポート
from data_engine.core.config import ARIA_SCOPE
from data_engine.core.network_utils import patch_all_networking
from data_engine.engines.merger_engine import MergerEngine
from data_engine.engines.worker_engine import WorkerEngine

# グローバルな実行環境の統制 (TQDM等) は CONFIG インポート時に適用済み

# 全体的な通信の堅牢化を適用
patch_all_networking()

logger.info(f"ARIA Execution Scope (from SSOT config): {ARIA_SCOPE}")


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

    Orchestrator Responsibility:
    - MergerEngine のインスタンス化と実行
    """
    engine = MergerEngine(catalog, run_id)
    return engine.run()
