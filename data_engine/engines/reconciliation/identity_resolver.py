import pandas as pd
from loguru import logger
from data_engine.core.utils import normalize_code

class IdentityResolver:
    """証券コードと EDINET コードの架け橋（Bridging）および破棄ルールを担当"""

    SPECIAL_MARKET_KEYWORDS = ["ETF", "REIT", "PRO MARKET"]

    def __init__(self, catalog_manager):
        self.cm = catalog_manager

    def bridge_fill(self, incoming_data: pd.DataFrame) -> pd.DataFrame:
        """【Identity Bridging】JPX レコードに EDINET コードを逆引き補完"""
        is_jpx_update = "sector_jpx_33" in incoming_data.columns
        if not is_jpx_update or not self.cm.edinet_codes:
            return incoming_data

        # 証券コード -> EDINET コード の逆引き辞書
        sec_to_edinet = {
            v.code: k for k, v in self.cm.edinet_codes.items() if v.code and v.code.startswith("JP:")
        }

        def fill_fn(row):
            if not row.get("edinet_code") and row.get("code") in sec_to_edinet:
                return sec_to_edinet[row["code"]]
            return row.get("edinet_code")

        incoming_data["edinet_code"] = incoming_data.apply(fill_fn, axis=1)
        return incoming_data

    def apply_disposal_rule(self, incoming_data: pd.DataFrame) -> pd.DataFrame:
        """【Disposal Rule】不要な JPX 重複レコードを排除"""
        is_jpx_update = "sector_jpx_33" in incoming_data.columns
        if not is_jpx_update:
            return incoming_data

        filtered_list = []
        discard_count = 0
        for _, row in incoming_data.iterrows():
            market = str(row.get("market") or "").upper()
            is_special = any(x in market for x in self.SPECIAL_MARKET_KEYWORDS)
            sec_code = str(row.get("code") or "")
            is_preferred = sec_code and sec_code[-1] != "0"
            has_edinet = pd.notna(row.get("edinet_code"))

            # 普通株式 (5桁目0) かつ EDINET未登録 かつ 特殊でない銘柄は破棄
            if not is_special and not is_preferred and not has_edinet:
                discard_count += 1
                continue
            filtered_list.append(row)

        if discard_count > 0:
            logger.info(f"🗑️ JPX 不要レコード破棄 (普通株式/EDINET未登録): {discard_count} 件")
        return pd.DataFrame(filtered_list)
