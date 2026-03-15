"""
Reconciliation Engine — ARIA の心臓部である名寄せ・生存判定・属性継承・スコープフィルタリングを担当するエンジン。

【工学的主権】リコンシリエーション・ロジックをモジュール化し、アイデンティティ解決とライフサイクル管理を分離。
"""

import datetime
import pandas as pd
from loguru import logger

from data_engine.core.models import StockMasterRecord
from data_engine.core.utils import normalize_code
from data_engine.engines.reconciliation import IdentityResolver, LifecycleManager


class ReconciliationEngine:
    """
    名寄せ・属性解決エンジン (Orchestrator)
    
    ARIA 統治の 5 鉄則に基づき、EDINET（SSOT）と JPX（補完）を統合します。
    """
    
    SPECIAL_MARKET_KEYWORDS = ["ETF", "REIT", "PRO MARKET"]

    def __init__(self, catalog_manager):
        """
        Args:
            catalog_manager (CatalogManager): カタログ管理インスタンス
        """
        self.cm = catalog_manager
        self.resolver = IdentityResolver(catalog_manager)
        self.lifecycle = LifecycleManager(catalog_manager)
        logger.debug("ReconciliationEngine (Modular) を初期化しました。")

    def sync_master_from_edinet_codes(self, results: dict = None) -> bool:
        """
        EDINETコードリストの結果をマスタに反映する（旧：update_master_from_edinet_codes の名称を統一）。
        """
        if results is None:
            # CatalogManager からの引数なし呼び出しに対応
            results = self.cm.edinet_codes

        if not results:
            return True

        incoming_recs = []
        for res in results.values():
            if isinstance(res, StockMasterRecord):
                incoming_recs.append(res.model_dump())
            elif hasattr(res, "model_dump"):
                incoming_recs.append(res.model_dump())
            else:
                incoming_recs.append(res)

        return self.update_stocks_master(pd.DataFrame(incoming_recs))

    def update_master_from_edinet_codes(self):
        """CatalogManager からの古い呼び出し形式への後方互換性"""
        return self.sync_master_from_edinet_codes()

    def update_stocks_master(self, incoming_data: pd.DataFrame):
        """
        マスタ更新 & 時系列リコンシリエーション (Sovereign Multi-Source Fusion)
        """
        if incoming_data.empty:
            return True

        def resolve_attr(group, col):
            vals = group[col].dropna()
            return vals.iloc[0] if not vals.empty else None

        today = datetime.datetime.now().strftime("%Y-%m-%d")

        # 1. 【Identity Bridging】証券コードと EDINET コードの架け橋
        incoming_data = self.resolver.bridge_fill(incoming_data)

        # 2. 【Disposal Rule】JPX 重複・不要レコードのフィルタリング (Ordinary Stock SSOT)
        incoming_data = self.resolver.apply_disposal_rule(incoming_data)

        # 3. 事前処理: プレフィックス正規化と親子紐付け
        processed_records = []
        current_codes_in_run = set()
        for _, row in incoming_data.iterrows():
            rec = row.to_dict()
            rec = {k: (v if not pd.isna(v) else None) for k, v in rec.items()}
            sec_code = rec.get("code")

            if sec_code:
                sec_code = normalize_code(sec_code, nationality="JP")
                rec["code"] = sec_code
                current_codes_in_run.add(sec_code)
                # 【Parenting】優先株の親紐付け
                rec = self.lifecycle.setup_parent_code(rec)

            # 既存属性の承継 (キャッシュ活用)
            if not self.cm.master_df.empty and sec_code:
                m_row = self.cm.master_df[self.cm.master_df["code"] == sec_code]
                if not m_row.empty:
                    m_rec = m_row.iloc[0].to_dict()
                    for k, v in m_rec.items():
                        if k not in rec or rec[k] is None:
                            rec[k] = v

            try:
                processed_records.append(StockMasterRecord(**rec).model_dump())
            except Exception as e:
                logger.error(f"銘柄バリデーション失敗 (code: {sec_code}): {e}")

        # 4. マージ処理
        incoming_df = pd.DataFrame(processed_records)
        current_m = self.cm.master_df.copy()
        all_states = pd.concat([current_m, incoming_df], ignore_index=True)

        # identity_key による物理的統合
        all_states["identity_key"] = all_states["edinet_code"].fillna(all_states["code"])
        
        best_records = []
        listing_events = []
        jpx_defs = []

        for _, group in all_states.groupby("identity_key", dropna=False):
            # 時系列ソート (最新優先)
            sorted_group = group.sort_values("last_submitted_at", ascending=False, na_position="last")
            latest_rec = sorted_group.iloc[0].copy()

            # 属性伝搬 (NULL 埋め)
            for attr in [
                "sector_jpx_33", "sector_33_code", "sector_jpx_17", "sector_17_code",
                "market", "size_code", "size_category", "jcn", "edinet_code", 
                "parent_code", "former_edinet_codes", "company_name_en", 
                "company_name_kana", "submitter_type", "address", 
                "industry_edinet", "industry_edinet_en", "capital", "settlement_date",
                "is_consolidated"
            ]:
                val = resolve_attr(sorted_group, attr)
                if val is not None:
                    latest_rec[attr] = val

            # JPX 定義の収集 (Dimension Table)
            self._collect_jpx_defs(latest_rec, jpx_defs)

            # 上場イベント検知
            listing_events.extend(self.lifecycle.detect_listing_events(latest_rec, current_m))

            best_records.append(latest_rec)

        # 5. 【Tracking】消失銘柄の判定
        new_master_df = pd.DataFrame(best_records)
        new_master_df = self.lifecycle.track_disappearance(new_master_df, current_codes_in_run)

        self.cm.master_df = new_master_df

        # メタデータの保存 (Master, Listing History, JPX Definitions)
        self._save_metadata(jpx_defs, listing_events)

        return self.cm.hf.save_and_upload("master", self.cm.master_df, clean_fn=self.cm._clean_dataframe, defer=True)

    def _collect_jpx_defs(self, rec, jpx_defs: list):
        """業種・規模名称のマッピング定義を収集（正規化のため）"""
        if rec.get("sector_33_code"):
            jpx_defs.append({"type": "sector_33", "code": rec["sector_33_code"], "name": rec["sector_jpx_33"]})
        if rec.get("sector_17_code"):
            jpx_defs.append({"type": "sector_17", "code": rec["sector_17_code"], "name": rec["sector_jpx_17"]})
        if rec.get("size_code"):
            jpx_defs.append({"type": "size", "code": rec["size_code"], "name": rec["size_category"]})

    def _save_metadata(self, jpx_defs, listing_events):
        """マスタ付随メタデータの保存"""
        if jpx_defs:
            df_defs = pd.DataFrame(jpx_defs).drop_duplicates(subset=["type", "code"])
            self.cm.hf.save_and_upload("jpx_definitions", df_defs, defer=True)

        if listing_events:
            events_df = pd.DataFrame(listing_events).drop_duplicates(subset=["code", "type"])
            self.cm.update_listing_history(events_df)

    def reconstruct_name_history(self, code: str) -> pd.DataFrame:
        """
        【Identity Sovereignty】カタログデータから特定の銘柄の社名変更履歴を決定論的に再構成。
        """
        if self.cm.catalog_df.empty:
            return pd.DataFrame()

        # 1. 該当コードの書類を抽出し、提出日時順にソート
        stock_docs = self.cm.catalog_df[self.cm.catalog_df["code"] == code].copy()
        if stock_docs.empty:
            return pd.DataFrame()

        stock_docs.sort_values("submit_at", ascending=True, inplace=True)

        # 2. 社名の遷移を検知
        history = []
        last_name = None
        for _, row in stock_docs.iterrows():
            curr_name = row.get("company_name")
            if not curr_name or pd.isna(curr_name):
                continue
            
            # 漢字名の正規化 (株), (有) 等の除去
            curr_name = self.normalize_company_name(curr_name)

            if last_name and curr_name != last_name:
                history.append({
                    "code": code,
                    "old_name": last_name,
                    "new_name": curr_name,
                    "change_date": str(row["submit_at"])[:10]
                })
            last_name = curr_name

        return pd.DataFrame(history)

    def normalize_company_name(self, name: str) -> str:
        """社名から法的形態の表記(株)などを除去し、純粋な商号を抽出"""
        if not name:
            return name
        for noise in ["株式会社", "有限会社", "合同会社", "（株）", "(株)", "（有）", "(有)"]:
            name = name.replace(noise, "")
        return name.strip()
