from pathlib import Path
from typing import Dict, List

import requests

# 内部モジュール（外部ライブラリ）のインポート
from edinet_xbrl_prep.edinet_xbrl_prep.edinet_api import edinet_response_metadata, request_term
from edinet_xbrl_prep.edinet_xbrl_prep.link_base_file_analyzer import account_list_common
from loguru import logger
from models import EdinetDocument


class EdinetEngine:
    def __init__(self, api_key: str, data_path: Path, taxonomy_urls: Dict[str, str] = None):
        self.api_key = api_key
        self.data_path = data_path
        self.taxonomy_urls = taxonomy_urls or {}
        self._apply_monkypatches()
        logger.info("EdinetEngine を初期化しました。")

    def _apply_monkypatches(self):
        """ライブラリのバグや制約を修正するためのパッチ適用"""
        from network_utils import patch_all_networking

        # 1. 全体的な通信の堅牢化を適用
        patch_all_networking()

        # 2. サブモジュール内の verify=False を物理的に無効化する
        import edinet_xbrl_prep.edinet_xbrl_prep.edinet_api as edinet_api_mod
        from network_utils import get_robust_session

        # get_edinet_metadata と request_doc の内部で直接 requests.Session().get している箇所をラップ

        # 共通の堅牢なセッションを取得
        robust_session = get_robust_session()

        class SecureSessionAdapter:
            """requests.Session の振る舞いを模倣しつつ、実体は共有の robust_session を使うアダプタ"""

            def __init__(self):
                self._session = robust_session

            def get(self, url, **kwargs):
                # EDINET関連のURLなら verify=True を強制
                if "api.edinet-fsa.go.jp" in url:
                    kwargs["verify"] = True
                return self._session.get(url, **kwargs)

            def mount(self, prefix, adapter):
                self._session.mount(prefix, adapter)

        # 影響範囲を限定するため、モジュール内の requests.Session を差し替える
        # edinet_xbrl_prep は `with requests.Session() as s:` のように使うため、
        # クラス自体を呼び出し可能なもの（コンストラクタ）として差し替える必要がある。
        edinet_api_mod.requests.Session = SecureSessionAdapter

        # account_list_common._download_taxonomy をモンキーパッチ
        # closureで self.taxonomy_urls を参照できるようにする
        from edinet_xbrl_prep.edinet_xbrl_prep.link_base_file_analyzer import account_list_common

        # インスタンスメソッドとして呼ばれるため、第一引数は self (account_list_commonのインスタンス)
        # self.taxonomy_urls は EdinetEngine のインスタンス変数なので、クロージャでキャプチャする
        captured_taxonomy_urls = self.taxonomy_urls

        def patched_download_taxonomy(obj):
            """モンキーパッチされたタクソノミダウンロードメソッド"""
            year = obj.account_list_year
            # taxonomy_urls に定義があればそれを使う
            url = captured_taxonomy_urls.get(str(year))

            if not url:
                # 【重要】定義がない場合はエラーとする (フォールバック禁止)
                msg = f"タクソノミURL定義が見つかりません (Year: {year})。taxonomy_urls.json を確認してください。"
                logger.error(msg)
                raise ValueError(msg)

            logger.info(f"タクソノミURL定義を使用します (Year: {year}): {url}")

            try:
                # ダウンロード (timeout指定を追加して堅牢化)
                r = requests.get(url, stream=True, timeout=(30, 180))
                r.raise_for_status()
                with obj.taxonomy_file.open(mode="wb") as f:
                    for chunk in r.iter_content(1024):
                        f.write(chunk)
                logger.success(f"タクソノミファイルのダウンロードに成功しました: {url}")
            except Exception as e:
                logger.error(f"タクソノミファイルのダウンロードに失敗しました (Year: {year}, URL: {url}): {e}")
                raise e

        account_list_common._download_taxonomy = patched_download_taxonomy

    def fetch_metadata(self, start_date: str, end_date: str, ope_date_time: str = None) -> List[Dict]:
        """
        指定期間の全書類メタデータを取得し、Pydanticでバリデーション
        ope_date_timeを指定すると、API V2 の増分同期機能を使用して差分のみを取得。
        """
        # 入力値の前後空白を除去 (Pydanticバリデーションエラー対策)
        start_date = start_date.strip() if isinstance(start_date, str) else start_date
        end_date = end_date.strip() if isinstance(end_date, str) else end_date

        logger.info(f"EDINETメタデータ取得開始: {start_date} ~ {end_date} (増分基準: {ope_date_time or 'なし'})")
        res_results = request_term(
            api_key=self.api_key, start_date_str=start_date, end_date_str=end_date, ope_date_time_str=ope_date_time
        )

        # 【修正】TSE URL を設定ファイルから読み込み (Issue-6: ハードコード排除)
        # TSE URL を config から読み込み (SSOT準拠)
        from config import TSE_URL

        tse_url = TSE_URL
        meta = edinet_response_metadata(tse_sector_url=tse_url, tmp_path_str=str(self.data_path))
        meta.set_data(res_results)

        df = meta.get_metadata_pandas_df()

        if df.empty:
            logger.warning("No documents found for the specified period.")
            return []

        records = df.to_dict("records")
        validated_records = []
        for rec in records:
            try:
                # Pydantic モデルでバリデーション & 正規化
                doc = EdinetDocument(**rec)
                validated_records.append(doc.model_dump(by_alias=True))
            except Exception as e:
                logger.error(f"Validation failed for metadata (docID: {rec.get('docID')}): {e}")

        logger.info(f"Metadata fetch completed: {len(validated_records)} documents")
        return validated_records

    def download_doc(self, doc_id: str, save_path: Path, doc_type: int = 1) -> bool:
        """書類をダウンロード保存 (1=XBRL, 2=PDF)"""
        url = f"https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
        params = {"type": doc_type, "Subscription-Key": self.api_key}

        try:
            # 【修正】堅牢セッションを使用してリトライ戦略を適用（MonkeyPatch回避のためGLOBALを使用）
            from network_utils import GLOBAL_ROBUST_SESSION

            session = GLOBAL_ROBUST_SESSION
            r = session.get(url, params=params, timeout=(20, 90), stream=True)
            if r.status_code == 200:
                save_path.parent.mkdir(parents=True, exist_ok=True)
                with open(save_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 64):
                        f.write(chunk)
                logger.debug(f"取得成功: {doc_id} (type={doc_type})")
                return True
            else:
                logger.error(f"DL失敗: {doc_id} (HTTP {r.status_code})")
                return False
        except Exception:
            logger.exception(f"DLエラー: {doc_id}")
            return False

    def get_account_list(self, taxonomy_year: str):
        """解析用タクソノミの取得"""
        try:
            # 原因追跡のため、渡される引数の型をログに出力
            logger.debug(
                f"タクソノミ取得試行: data_path={self.data_path} (type: {type(self.data_path)}), year={taxonomy_year}"
            )

            # ライブラリ内部で / 演算子が使用されているため、Path オブジェクトをそのまま渡す
            acc = account_list_common(self.data_path, taxonomy_year)
            return acc
        except Exception:
            logger.exception(f"タクソノミ取得エラー (Year: {taxonomy_year})")
            return None
