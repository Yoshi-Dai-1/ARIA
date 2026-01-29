from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from loguru import logger


class HistoryEngine:
    def __init__(self, data_path: Path):
        self.data_path = data_path
        self.jpx_url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
        self.nikkei_url = "https://indexes.nikkei.co.jp/nkave/statistics/datalist/constituent?list=225&type=csv"
        logger.info("HistoryEngine を初期化しました。")

    def fetch_jpx_master(self) -> pd.DataFrame:
        """JPXから最新の銘柄一覧を取得"""
        logger.info("JPX銘柄マスタを取得中...")
        r = requests.get(self.jpx_url, stream=True)
        self.data_path.mkdir(parents=True, exist_ok=True)
        xls_path = self.data_path / "jpx_master.xls"
        with xls_path.open("wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)

        df = pd.read_excel(xls_path, dtype={"コード": str})
        df = df.rename(
            columns={"コード": "code", "銘柄名": "company_name", "33業種区分": "sector", "市場・商品区分": "market"}
        )
        df["code"] = df["code"].str[:4]
        logger.success(f"JPXマスタ取得完了: {len(df)} 銘柄")
        return df[["code", "company_name", "sector", "market"]]

    def generate_listing_events(
        self,
        old_master: pd.DataFrame,
        new_master: pd.DataFrame,
        old_history: pd.DataFrame = None,
    ) -> pd.DataFrame:
        """マスタの差分から上場・廃止イベントを生成 (再上場判定含む)"""
        if old_master.empty:
            return pd.DataFrame()

        old_codes = set(old_master["code"])
        new_codes = set(new_master["code"])

        events = []
        today = datetime.now().strftime("%Y-%m-%d")

        # 過去に廃止されたコードのセットを作成
        delisted_codes = set()
        if old_history is not None and not old_history.empty:
            delisted_items = old_history[old_history["type"] == "DELISTING"]
            delisted_codes = set(delisted_items["code"])

        # 新規上場 (または再上場)
        for code in new_codes - old_codes:
            event_type = "RE-LISTING" if code in delisted_codes else "LISTING"
            # 理由の手動管理は廃止。システム判定のみを記録。
            events.append({"code": code, "type": event_type, "event_date": today})

        # 廃止
        for code in old_codes - new_codes:
            # 理由は自動取得不可のため記録しない
            events.append({"code": code, "type": "DELISTING", "event_date": today})

        return pd.DataFrame(events)

    def fetch_nikkei_225_events(self) -> pd.DataFrame:
        """日経225構成銘柄を取得（簡易版）"""
        # 実際にはCSVのパーシングが必要だが、ここでは空リストまたは簡易取得を想定
        return pd.DataFrame(columns=["code"])

    def generate_index_events(
        self,
        index_name: str,
        old_list: pd.DataFrame,
        new_list: pd.DataFrame,
        old_history: pd.DataFrame = None,
    ) -> pd.DataFrame:
        """指数採用・除外イベント生成 (再採用判定含む)"""
        old_set = set(old_list["code"]) if not old_list.empty else set()
        new_set = set(new_list["code"]) if not new_list.empty else set()

        events = []
        today = datetime.now().strftime("%Y-%m-%d")

        # 過去に除外されたコードのセット
        removed_codes = set()
        if old_history is not None and not old_history.empty:
            # 同一指数の過去の除外履歴を検索
            removed_items = old_history[(old_history["index_name"] == index_name) & (old_history["type"] == "REMOVE")]
            removed_codes = set(removed_items["code"])

        for code in new_set - old_set:
            event_type = "RE-INCLUSION" if code in removed_codes else "ADD"
            events.append({"index_name": index_name, "code": code, "type": event_type, "event_date": today})

        for code in old_set - new_set:
            events.append({"index_name": index_name, "code": code, "type": "REMOVE", "event_date": today})

        return pd.DataFrame(events)
