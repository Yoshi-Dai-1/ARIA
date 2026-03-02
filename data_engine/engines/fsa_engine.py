import io
import zipfile
from typing import Dict, Optional, Tuple

import pandas as pd
import requests
from loguru import logger

from data_engine.core.config import CONFIG
from data_engine.core.models import EdinetCodeRecord
from data_engine.core.network_utils import GLOBAL_ROBUST_SESSION


class FsaEngine:
    """
    金融庁（FSA）が提供する各種リスト（EDINETコードリスト、提出者集約一覧等）の
    取得、解析、および同期を専門に行うエンジン。
    """

    def __init__(self, session: Optional[requests.Session] = None):
        # 堅牢な共通セッションを優先し、なければ GLOBAL を使用
        self.session = session or GLOBAL_ROBUST_SESSION
        logger.info("FsaEngine を初期化しました。")

    def sync_edinet_code_lists(self) -> Tuple[Dict[str, EdinetCodeRecord], Dict[str, str]]:
        """
        金融庁から最新の EDINET コードリストと集約一覧を取得し、マッピングオブジェクトを生成する。
        """
        logger.info("金融庁からEDINETコードリストを取得・解析中...")
        codes = {}
        aggregation_map = {}

        # --- 集約一覧 (aggregation_map) の取得 ---
        # 企業合併などでEDINETコードが変わった場合の追跡用
        # 2026対応: 直接CSVダウンロード可能な静的URLを使用
        url_consolidated = "https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/download/ESE140190.csv"
        try:
            res_c = self.session.get(url_consolidated, timeout=15)
            if res_c.status_code == 200:
                # ZIPではなく直接CSVとして読み込む (Shift-JIS/CP932)
                from io import StringIO

                content = res_c.content.decode("cp932")
                df_agg = pd.read_csv(StringIO(content), skiprows=1)
                for _, row in df_agg.iterrows():
                    o_code = str(row.get("提出者等のEDINETコード（変更前）", "")).strip()
                    n_code = str(row.get("提出者等のEDINETコード（変更後）", "")).strip()
                    if len(o_code) == 6 and len(n_code) == 6:
                        aggregation_map[o_code] = n_code
                logger.info(f"提出者集約一覧を取得しました: {len(aggregation_map)}件の「旧→新」マッピング")
        except Exception as e:
            logger.warning(f"提出者集約一覧の取得に失敗しました: {e}。集約ブリッジ機能はスキップされます。")

        # --- 和文リスト (JCN, 上場区分, 業種等) ---
        # 2026対応: API v2 の edinetcodelist エンドポイントは ZIP 直送ではなく JSON (URL 含む) を返す
        url_meta = "https://api.edinet-fsa.go.jp/api/v2/documents/edinetcodelist"
        params = {"Subscription-Key": CONFIG.EDINET_API_KEY}
        try:
            res_meta = self.session.get(url_meta, params=params, timeout=15)
            if res_meta.status_code == 200:
                meta_data = res_meta.json()
                # 和文リストの ZIP URL
                url_ja = meta_data.get("edinetCodeListUrl")
                # 英文リストの CSV URL (v2 では ZIP ではなく直接 CSV の場合がある)
                url_en = meta_data.get("industryCodeListUrl")

                if url_ja:
                    res = self.session.get(url_ja, timeout=30)
                    if res.status_code == 200:
                        with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                            csv_filename = [f for f in z.namelist() if f.endswith(".csv")][0]
                            with z.open(csv_filename) as f:
                                df_ja = pd.read_csv(f, encoding="cp932", skiprows=1)
                                for _, row in df_ja.iterrows():
                                    e_code = str(row.get("ＥＤＩＮＥＴコード", "")).strip()
                                    if len(e_code) != 6:
                                        continue
                                    sec_code = self._safe_int_str(row.get("証券コード"))
                                    # API側の "0" という異常な証券コードを排除
                                    if sec_code in ("0", "0000", "00000"):
                                        sec_code = None
                                    jcn = self._safe_int_str(row.get("提出者法人番号"))
                                    codes[e_code] = EdinetCodeRecord(
                                        edinet_code=e_code,
                                        jcn=jcn,
                                        submitter_type=row.get("提出者種別"),
                                        is_listed=row.get("上場区分"),
                                        is_consolidated=row.get("連結の有無"),
                                        capital=self._safe_int_str(row.get("資本金")),
                                        settlement_date=str(row.get("決算日") or "").strip() or None,
                                        submitter_name=str(row.get("提出者名") or "").strip() or None,
                                        submitter_name_en=str(row.get("提出者名（英字）") or "").strip() or None,
                                        submitter_name_kana=str(row.get("提出者名（ヨミ）") or "").strip() or None,
                                        address=str(row.get("所在地") or "").strip() or None,
                                        industry_edinet=str(row.get("提出者業種") or "").strip() or None,
                                        sec_code=sec_code,
                                    )

                # --- 英文リストの反映 (v2 メタデータから取得したURLを使用) ---
                if url_en:
                    try:
                        res_en = self.session.get(url_en, timeout=15)
                        if res_en.status_code == 200:
                            # v2業界コードは ZIP ではなく直接 CSV の可能性がある
                            if b"PK" in res_en.content[:2]:
                                with zipfile.ZipFile(io.BytesIO(res_en.content)) as z:
                                    csv_filename = [f for f in z.namelist() if f.endswith(".csv")][0]
                                    with z.open(csv_filename) as f:
                                        df_en = pd.read_csv(f, encoding="cp932", skiprows=1)
                            else:
                                from io import StringIO

                                df_en = pd.read_csv(StringIO(res_en.content.decode("cp932")), skiprows=1)

                            for _, row in df_en.iterrows():
                                e_code = str(row.get("Edinet Code") or row.get("ＥＤＩＮＥＴコード") or "").strip()
                                ind_en = str(row.get("Industry") or row.get("提出者業種") or "").strip() or None
                                if e_code in codes and ind_en:
                                    codes[e_code].industry_edinet_en = ind_en
                    except Exception as e:
                        logger.warning(f"英文EDINETコードリストの取得・反映に失敗しました: {e}")
            else:
                logger.error(f"EDINETコードリストメタデータの取得失敗: HTTP {res_meta.status_code}")
                return {}, {}
        except Exception as e:
            logger.error(f"EDINETコードリストの処理中にエラー: {e}")
            return {}, {}

        return codes, aggregation_map

    def _safe_int_str(self, val) -> Optional[str]:
        """数値を安全に文字列化（NaNや空文字はNone）"""
        if pd.isna(val) or val is None or str(val).strip() == "":
            return None
        try:
            # 浮動小数点経由で整数文字列に変換 (123.0 -> "123")
            return str(int(float(val)))
        except ValueError:
            return str(val).strip()
