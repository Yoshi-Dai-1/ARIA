import pandas as pd
from loguru import logger


class IdentityResolver:
    """証券コードと EDINET コードの架け橋（Bridging）および破棄ルールを担当"""

    SPECIAL_MARKET_KEYWORDS = ["ETF", "REIT", "PRO MARKET"]

    def __init__(self, catalog_manager):
        self.cm = catalog_manager

    def resolve_master_from_edinet(self, edinet_codes: dict) -> pd.DataFrame:
        """EDINET コードリスト (Dict of Pydantic models) を DataFrame に変換"""
        recs = []
        for res in edinet_codes.values():
            if hasattr(res, "model_dump"):
                recs.append(res.model_dump())
            else:
                recs.append(res)
        return pd.DataFrame(recs)

    def bridge_fill(self, incoming_data: pd.DataFrame) -> pd.DataFrame:
        """【Identity Bridging】JPX レコードに EDINET コードを逆引き補完"""
        is_jpx_update = "sector_jpx_33" in incoming_data.columns
        if not is_jpx_update or not self.cm.edinet_codes:
            return incoming_data

        # 証券コード -> EDINET コード の逆引き辞書 (Pydanticモデルと辞書の両方に対応)
        sec_to_edinet = {}
        for k, v in self.cm.edinet_codes.items():
            c = getattr(v, "code", None) if hasattr(v, "code") else v.get("code")
            if c and str(c).startswith("JP:"):
                sec_to_edinet[c] = k

        if not sec_to_edinet:
            logger.warning("EDINETコードの逆引き辞書が空です。補完をスキップします。")
            return incoming_data

        def fill_fn(row):
            e_code = row.get("edinet_code")
            if (pd.isna(e_code) or e_code is None) and row.get("code") in sec_to_edinet:
                return sec_to_edinet[row["code"]]
            return e_code

        incoming_data["edinet_code"] = incoming_data.apply(fill_fn, axis=1)
        return incoming_data

    def apply_disposal_rule(self, incoming_data: pd.DataFrame) -> pd.DataFrame:
        """【Disposal Rule】不要な JPX 重複レコードを排除"""
        is_jpx_update = "sector_jpx_33" in incoming_data.columns
        if not is_jpx_update:
            return incoming_data

        filtered_list = []
        discarded_details = []
        for _, row in incoming_data.iterrows():
            market = str(row.get("market") or "").upper()
            is_special = any(x in market for x in self.SPECIAL_MARKET_KEYWORDS)
            sec_code = str(row.get("code") or "")
            is_preferred = sec_code and sec_code[-1] != "0"
            has_edinet = pd.notna(row.get("edinet_code"))

            # 普通株式 (5桁目0) かつ EDINET未登録 かつ 特殊でない銘柄は破棄
            if not is_special and not is_preferred and not has_edinet:
                discarded_details.append(f"{sec_code} ({row.get('company_name')})")
                continue
            filtered_list.append(row)

        if discarded_details:
            logger.info(f"🗑️ JPX 不要レコード破棄 (普通株式/EDINET未登録): {len(discarded_details)} 件")
            # ログの肥大化を防ぎつつ、透明性を確保するため、最初の20件を表示
            sample_size = 20
            sample = discarded_details[:sample_size]
            logger.info(f"破棄銘柄: {', '.join(sample)}{' ...' if len(discarded_details) > sample_size else ''}")

        return pd.DataFrame(filtered_list)
