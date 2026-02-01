from pathlib import Path
from typing import Dict, List

import requests
from loguru import logger

# 内部モジュール（外部ライブラリ）のインポート
from edinet_xbrl_prep.edinet_xbrl_prep.edinet_api import edinet_response_metadata, request_term
from edinet_xbrl_prep.edinet_xbrl_prep.link_base_file_analyzer import account_list_common
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

        # サブモジュール内の verify=False を物理的に無効化する
        import edinet_xbrl_prep.edinet_xbrl_prep.edinet_api as edinet_api_mod

        # get_edinet_metadata と request_doc の内部で直接 requests.Session().get している箇所をラップ
        original_session = requests.Session

        class SecureSession(original_session):
            def get(self, url, **kwargs):
                # EDINET関連のURLなら verify=True を強制
                if "api.edinet-fsa.go.jp" in url:
                    kwargs["verify"] = True
                return super().get(url, **kwargs)

        # 影響範囲を限定するため、モジュール内の requests.Session を差し替える
        edinet_api_mod.requests.Session = SecureSession

        # account_list_common._download_taxonomy をモンキーパッチ
        # closureで self.taxonomy_urls を参照できるようにする
        from edinet_xbrl_prep.edinet_xbrl_prep.link_base_file_analyzer import account_list_common

        original_download_taxonomy = account_list_common._download_taxonomy

        # インスタンスメソッドとして呼ばれるため、第一引数は self (account_list_commonのインスタンス)
        # self.taxonomy_urls は EdinetEngine のインスタンス変数なので、クロージャでキャプチャする
        captured_taxonomy_urls = self.taxonomy_urls

        def patched_download_taxonomy(obj):
            """モンキーパッチされたタクソノミダウンロードメソッド"""
            year = obj.account_list_year
            # taxonomy_urls に定義があればそれを使う、なければオリジナルの辞書（ハードコード）にフォールバック...したいが
            # オリジナルのメソッドは内部で辞書を定義しているため、外から注入できない。
            # したがって、ここでURL解決ロジックを上書きする。

            # taxonomy_urls からURLを取得
            url = captured_taxonomy_urls.get(str(year))

            if not url:
                # 定義がない場合は既存のロジック（ハードコード辞書）に任せたいが、
                # オリジナルメソッドを呼ぶとハードコード辞書が使われる。
                # ただし、オリジナルメソッドは URL を引数で取れず、内部で辞書を持っている。
                # 仕方ないので、定義がある場合のみ上書きし、ない場合はオリジナルを呼ぶ。
                logger.info(f"タクソノミURL定義が見つからないため、デフォルト動作を使用します (Year: {year})")
                return original_download_taxonomy(obj)

            logger.info(f"タクソノミURL定義を使用します (Year: {year}): {url}")

            # 以下、オリジナルメソッドのダウンロード処理と同等の実装
            # requests.get はこのファイルの冒頭で import されているモジュールを使う
            # obj.taxonomy_file は account_list_common のインスタンス変数

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
                # 失敗した場合はオリジナルを試すべきか？ -> 二重ダウンロードになるので避ける。エラーで終了させる。
                raise e

        account_list_common._download_taxonomy = patched_download_taxonomy

    def fetch_metadata(self, start_date: str, end_date: str) -> List[Dict]:
        """指定期間の全書類メタデータを取得し、Pydanticでバリデーション"""
        logger.info(f"EDINETメタデータ取得開始: {start_date} ~ {end_date}")
        res_results = request_term(api_key=self.api_key, start_date_str=start_date, end_date_str=end_date)

        tse_url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
        meta = edinet_response_metadata(tse_sector_url=tse_url, tmp_path_str=str(self.data_path))
        meta.set_data(res_results)

        df = meta.get_metadata_pandas_df()
        if df.empty:
            logger.warning("対象期間の書類は見つかりませんでした。")
            return []

        records = df.to_dict("records")
        validated_records = []
        for rec in records:
            try:
                # Pydantic モデルでバリデーション & 正規化
                doc = EdinetDocument(**rec)
                validated_records.append(doc.model_dump(by_alias=True))
            except Exception as e:
                logger.error(f"書類メタデータのバリデーション失敗 (docID: {rec.get('docID')}): {e}")

        logger.success(f"メタデータ取得完了: {len(validated_records)} 件")
        return validated_records

    def download_doc(self, doc_id: str, save_path: Path, doc_type: int = 1) -> bool:
        """書類をダウンロード保存 (1=XBRL, 2=PDF)"""
        url = f"https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
        params = {"type": doc_type, "Subscription-Key": self.api_key}

        try:
            r = requests.get(url, params=params, timeout=(20, 90), stream=True)
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
