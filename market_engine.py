import io
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd
import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential


class IndexStrategy(ABC):
    """指数ごとのデータ取得ロジックの基底クラス"""

    @abstractmethod
    def fetch_data(self) -> pd.DataFrame:
        """
        構成銘柄データを取得する
        Returns:
            pd.DataFrame: columns=["code", "weight"] (sector等は含まない)
        """
        pass


class NikkeiStrategy(IndexStrategy):
    """日経225 (Nikkei Source)"""

    def __init__(self):
        self.url = "https://indexes.nikkei.co.jp/nkave/statistics/datalist/constituent?list=225&type=csv"
        # 403 Forbidden 対策: よりブラウザに近いヘッダセットを使用
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
                "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
            ),
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": "https://indexes.nikkei.co.jp/nkave/statistics/datalist/constituent?list=225&type=csv",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        }

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=4, max=20))
    def fetch_data(self) -> pd.DataFrame:
        logger.info("日経225構成銘柄を取得中...")
        try:
            r = requests.get(self.url, headers=self.headers, timeout=60)
            if r.status_code != 200:
                logger.error(f"日経225取得エラー: HTTP {r.status_code}")
                r.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"日経225リクエスト失敗: {e}")
            raise

        # Shift-JISでデコード
        # 想定CSV: 日付,コード,銘柄名,業種,構成比率(%)
        # ヘッダ行があるか確認が必要だが、pandasで読むのが安全
        try:
            # skiprowsなどは実際のCSVに合わせて調整。通常は1行目がヘッダ。
            df = pd.read_csv(io.BytesIO(r.content), encoding="shift_jis")

            # カラム名マッピングの推測 (CSVが日本語ヘッダの場合)
            # "コード", "構成比率(%)" などを探す
            code_col = next((c for c in df.columns if "コード" in c), None)
            weight_col = next((c for c in df.columns if "構成比率" in c or "Weight" in c), None)

            if not code_col or not weight_col:
                raise ValueError(f"必須カラムが見つかりません。Columns: {df.columns}")

            df = df[[code_col, weight_col]].rename(columns={code_col: "code", weight_col: "weight"})

            # 型変換
            df["code"] = df["code"].astype(str).str.strip()

            # Weight: "2.51%" -> 0.0251
            def parse_weight(x):
                if isinstance(x, str):
                    clean = x.replace("%", "").strip()
                    if not clean:
                        return 0.0
                    return float(clean) / 100.0
                return float(x)

            df["weight"] = df["weight"].apply(parse_weight)

            logger.success(f"日経225データ取得成功: {len(df)} 件")
            return df[["code", "weight"]]

        except Exception as e:
            logger.error(f"日経225パース失敗: {e}")
            raise e


class TopixStrategy(IndexStrategy):
    """TOPIX (JPX Source)"""

    def __init__(self):
        self.url = "https://www.jpx.co.jp/automation/markets/indices/topix/files/topixweight_j.csv"

    def fetch_data(self) -> pd.DataFrame:
        logger.info("TOPIX構成銘柄を取得中...")
        r = requests.get(self.url)
        r.raise_for_status()

        try:
            # JPX CSV confirmed as Shift-JIS
            df = pd.read_csv(io.BytesIO(r.content), encoding="shift_jis")

            # 想定カラム: 日付,銘柄名,コード,業種,TOPIXに占める個別銘柄のウエイト,ニューインデックス区分
            code_col = next((c for c in df.columns if "コード" in c), None)
            weight_col = next((c for c in df.columns if "ウエイト" in c), None)

            if not code_col or not weight_col:
                raise ValueError(f"TOPIX必須カラム欠落: {df.columns}")

            df = df[[code_col, weight_col]].rename(columns={code_col: "code", weight_col: "weight"})

            # 型変換
            df["code"] = df["code"].astype(str).str.strip()

            def parse_weight(x):
                if isinstance(x, str):
                    clean = x.replace("%", "").strip()
                    if not clean:
                        return 0.0
                    return float(clean) / 100.0
                return float(x)

            df["weight"] = df["weight"].apply(parse_weight)

            logger.success(f"TOPIXデータ取得成功: {len(df)} 件")
            return df[["code", "weight"]]

        except Exception as e:
            logger.error(f"TOPIXパース失敗: {e}")
            raise e


class MarketDataEngine:
    def __init__(self, data_path: Path):
        self.data_path = data_path
        self.strategies: Dict[str, IndexStrategy] = {
            "Nikkei225": NikkeiStrategy(),
            "TOPIX": TopixStrategy(),
        }
        self.jpx_url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def fetch_jpx_master(self) -> pd.DataFrame:
        """JPXから最新の銘柄一覧を取得 (Retry付き)"""
        logger.info("JPX銘柄マスタを取得中...")
        r = requests.get(self.jpx_url, stream=True)
        r.raise_for_status()

        # 保存して読み込む (Excel形式のため)
        self.data_path.mkdir(parents=True, exist_ok=True)
        xls_path = self.data_path / "jpx_master.xls"
        with xls_path.open("wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)

        df = pd.read_excel(xls_path, dtype={"コード": str})
        # カラムマッピング
        df = df.rename(
            columns={"コード": "code", "銘柄名": "company_name", "33業種区分": "sector", "市場・商品区分": "market"}
        )
        df["code"] = df["code"].str[:4]
        return df[["code", "company_name", "sector", "market"]]

    def update_listing_history(
        self, old_master: pd.DataFrame, new_master: pd.DataFrame, old_history: pd.DataFrame = None
    ) -> pd.DataFrame:
        """上場・廃止イベントの生成 (Listing History)"""
        # ロジックは従来のHistoryEngineと同じだが、今回は分離実装
        if old_master.empty:
            return pd.DataFrame()

        old_codes = set(old_master["code"])
        new_codes = set(new_master["code"])
        events = []
        today = datetime.now().strftime("%Y-%m-%d")

        # 過去の廃止銘柄
        delisted_codes = set()
        if old_history is not None and not old_history.empty:
            delisted = old_history[old_history["type"] == "DELISTING"]
            delisted_codes = set(delisted["code"])

        # 新規・再上場
        for code in new_codes - old_codes:
            event_type = "RE-LISTING" if code in delisted_codes else "LISTING"
            events.append({"code": code, "type": event_type, "event_date": today})

        # 廃止
        for code in old_codes - new_codes:
            events.append({"code": code, "type": "DELISTING", "event_date": today})

        return pd.DataFrame(events)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def fetch_index_data(self, index_name: str) -> pd.DataFrame:
        """指数データの取得 (Retry付き)"""
        if index_name not in self.strategies:
            raise ValueError(f"未対応の指数: {index_name}")

        return self.strategies[index_name].fetch_data()

    def generate_index_diff(
        self, index_name: str, old_const: pd.DataFrame, new_const: pd.DataFrame, date_str: str
    ) -> pd.DataFrame:
        """指数イベント差分生成 (ADD, REMOVE, UPDATE)"""
        events = []

        # 辞書化 {code: weight}
        old_map = dict(zip(old_const["code"], old_const["weight"], strict=False)) if not old_const.empty else {}
        new_map = dict(zip(new_const["code"], new_const["weight"], strict=False)) if not new_const.empty else {}

        old_keys = set(old_map.keys())
        new_keys = set(new_map.keys())

        # ADD
        for code in new_keys - old_keys:
            events.append(
                {
                    "date": date_str,
                    "index_name": index_name,
                    "code": code,
                    "type": "ADD",
                    "old_value": None,
                    "new_value": new_map[code],
                }
            )

        # REMOVE
        for code in old_keys - new_keys:
            events.append(
                {
                    "date": date_str,
                    "index_name": index_name,
                    "code": code,
                    "type": "REMOVE",
                    "old_value": old_map[code],
                    "new_value": None,
                }
            )

        # UPDATE (共通部分でウエイト変化)
        for code in new_keys & old_keys:
            v_old = old_map[code]
            v_new = new_map[code]
            # 浮動小数点比較 (許容誤差 1e-6)
            if abs(v_old - v_new) > 1e-6:
                events.append(
                    {
                        "date": date_str,
                        "index_name": index_name,
                        "code": code,
                        "type": "UPDATE",
                        "old_value": v_old,
                        "new_value": v_new,
                    }
                )

        return pd.DataFrame(events)
