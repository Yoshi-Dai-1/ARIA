import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def get_robust_session(
    retries: int = 5, backoff_factor: float = 2.0, status_forcelist: list = None, timeout: tuple = (20, 60)
) -> requests.Session:
    """
    リトライロジックを組み込んだ堅牢な Session オブジェクトを返す。

    Args:
        retries (int): 最大リトライ回数
        backoff_factor (float): 指数バックオフの係数
        status_forcelist (list): リトライ対象のHTTPステータスコード
        timeout (tuple): (connect_timeout, read_timeout) デフォルト値

    Returns:
        requests.Session: 設定済みのセッション
    """
    if status_forcelist is None:
        status_forcelist = [429, 500, 502, 503, 504]

    session = requests.Session()

    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST"],  # POSTも含める
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # タイムアウトをデフォルトで適用するためのフック
    # requests.Session 自体には default_timeout がないため、アダプターかモンキーパッチが必要だが、
    # ここでは利用側が session.get() を呼ぶときに意識しなくて済むよう、requestメソッドをラップする形も考えられる。
    # しかし、シンプルに Session を返す方が汎用性が高い。
    # timeout は呼び出し側で kwargs として渡すのが一般的だが、
    # ここでは利便性のため、session.request をフックしてデフォルトタイムアウトを設定する。

    original_request = session.request

    def robust_request(method, url, **kwargs):
        # httpx 互換の引数 (huggingface_hub 等で使用される) を requests 互換に変換
        if "follow_redirects" in kwargs:
            kwargs["allow_redirects"] = kwargs.pop("follow_redirects")

        if "timeout" not in kwargs:
            kwargs["timeout"] = timeout
        return original_request(method, url, **kwargs)

    session.request = robust_request

    return session


def patch_all_networking():
    """
    プロジェクトで使用されているすべての主要な通信ライブラリに対し、
    urllib3ベースのリトライ戦略を備えたセッションを強制注入する。
    これこそが世界最高水準の安定性を実現する唯一の方法である。
    """
    from loguru import logger

    robust_session = get_robust_session()

    # 1. HuggingFace Hub の通信を堅牢化
    try:
        import huggingface_hub.utils._http as hf_http

        # 内部的な get_session を差し替える
        hf_http.get_session = lambda: robust_session
        logger.info("HF Hub communication has been robustified.")
    except ImportError:
        pass

    # 2. 外部ライブラリ edinet_xbrl_prep の通信を堅牢化
    modules_to_patch = [
        "edinet_xbrl_prep.edinet_xbrl_prep.edinet_api",
        "edinet_xbrl_prep.edinet_xbrl_prep.link_base_file_analyzer",
        "edinet_xbrl_prep.edinet_xbrl_prep.fs_tbl",
    ]

    for mod_name in modules_to_patch:
        try:
            import sys

            if mod_name in sys.modules:
                mod = sys.modules[mod_name]
                if hasattr(mod, "requests"):
                    # Session クラス自体を差し替えて、インスタンス化時にアダプタ等が適用されるようにする
                    # (edinet_engine.py での実装と同様の考え方)
                    class RobustSessionAdapter:
                        def __init__(self, *args, **kwargs):
                            self._session = robust_session

                        def __getattr__(self, name):
                            return getattr(self._session, name)

                        def __enter__(self):
                            return self._session

                        def __exit__(self, *args):
                            pass

                    mod.requests.Session = RobustSessionAdapter

                    # 3. トップレベル関数の差し替え (直接呼び出し対策)
                    mod.requests.get = robust_session.get
                    mod.requests.post = robust_session.post
                    mod.requests.put = robust_session.put
                    mod.requests.delete = robust_session.delete
                    mod.requests.patch = robust_session.patch
                    mod.requests.head = robust_session.head
                    mod.requests.request = robust_session.request

                    logger.info(f"Patched all networking entry points in {mod_name}")
        except Exception as e:
            logger.debug(f"Failed to patch {mod_name}: {e}")

    logger.debug("Network patching completed.")
