"""
Reconciliation Engine — ARIA の心臓部である名寄せ・生業判定・属性継承・スコープフィルタリングを担当するエンジン。

CatalogManager から分離されたビジネスロジック:
- EDINETコードリストからのマスター同期 (sync/update_master_from_edinet_codes)
- JPXなど外部からのデータ注入とリコンシリエーション (update_stocks_master)
- 未知の銘柄の IPO 動的発見 (discover_edinet_code)
- 名前の正規化 (_normalize_company_name)
"""

import datetime
import re
import unicodedata
from typing import Optional, Tuple

import pandas as pd
from loguru import logger

from data_engine.core.models import StockMasterRecord
from data_engine.core.utils import normalize_code


class ReconciliationEngine:
    """名寄せ・属性解決エンジン"""
    
    SPECIAL_MARKET_KEYWORDS = ["ETF", "REIT", "PRO MARKET"]

    def __init__(self, catalog_manager):
        """
        Args:
            catalog_manager: CatalogManager インスタンス (状態アクセスのため)
        """
        self.cm = catalog_manager

    def normalize_company_name(self, name: str) -> str:
        """比較判定のために法人格や空白を除去して正規化する (NFKC対応版)"""
        if not name or not isinstance(name, str):
            return ""

        n = unicodedata.normalize("NFKC", name)
        n = n.replace(" ", "").replace("　", "")

        patterns = [
            r"株式会社",
            r"有限会社",
            r"合同会社",
            r"合資会社",
            r"合名会社",
            r"一般社団法人",
            r"一般財団法人",
            r"公益社団法人",
            r"公益財団法人",
            r"\(株\)",
            r"\(有\)",
            r"\(合\)",
            r"\(社\)",
            r"\(財\)",
        ]
        for p in patterns:
            n = re.sub(p, "", n)

        return n.strip()

    def discover_edinet_code(self, sec_code: str, name: Optional[str] = None) -> Optional[Tuple[str, str]]:
        """EDINET書類一覧API (V2) をスキャニングし、証券コードからEDINETコード/JCNを特定する"""
        display_name = f" ({name})" if name else ""
        logger.debug(f"証券コード {sec_code}{display_name} の EDINET情報を書類一覧API (V2) から探索中...")

        sec_code_5 = normalize_code(sec_code, nationality="JP")

        # 優先株などの場合は親銘柄から継承を試める
        # JP:12341 -> JP:12340 のようにプレフィックスを維持して親を求める
        if ":" in sec_code_5:
            prefix, core = sec_code_5.split(":", 1)
            if core[-1] != "0":
                parent_code = f"{prefix}:{core[:4]}0"
                # CatalogManager の tolerance 向上に合わせ、一貫した検索を行う
                parent_row = self.cm.master_df[self.cm.master_df["code"] == parent_code]
                if parent_row.empty:
                    # Legacy fallback
                    parent_row = self.cm.master_df[self.cm.master_df["code"] == f"{core[:4]}0"]

                if not parent_row.empty and pd.notna(parent_row.iloc[0].get("edinet_code")):
                    logger.debug(f"優先株 {sec_code_5} の EDINETコードを親銘柄 {parent_code} から継承します。")
                    return parent_row.iloc[0]["edinet_code"], parent_row.iloc[0].get("jcn")

        # 直近30日間をスキャン
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=30)

        # EdinetEngine が無効化されている場合は探索不可
        if not self.cm.edinet:
            logger.warning(f"EdinetEngine が無効なため、{sec_code_5} の EDINET 探索をスキップします。")
            return None

        try:
            # EdinetEngine経由でAPI V2を叩く
            meta_list = self.cm.edinet.fetch_metadata(
                start_date=start_date.strftime("%Y-%m-%d"), end_date=end_date.strftime("%Y-%m-%d")
            )

            for doc in meta_list:
                doc_sec = str(doc.get("secCode", "")).strip()
                if doc_sec == sec_code_5:
                    e_code = doc.get("edinetCode")
                    jcn = doc.get("JCN")
                    if e_code:
                        logger.success(f"発見 (V2): {sec_code_5} -> {e_code} (JCN: {jcn})")
                        return e_code, jcn
        except Exception as e:
            logger.error(f"書類API (V2) 探索中のエラー: {e}")

        return None

    def update_master_from_edinet_codes(self):
        """同期した edinet_codes および aggregation_map を master_df に反映させ、属性を最新化する"""
        logger.info("EDINETコードリストをマスタに反映中 (集約ブリッジ + JCN変更検知 + 上場生死判定)...")
        updated_count = 0
        listing_events = []
        today = datetime.datetime.now().strftime("%Y-%m-%d")

        master_dict = {
            str(row["edinet_code"]): row.to_dict()
            for _, row in self.cm.master_df.iterrows()
            if pd.notna(row.get("edinet_code"))
        }

        for e_code, ed_rec in self.cm.edinet_codes.items():
            is_listed_official = str(ed_rec.is_listed or "").strip() == "上場"

            if e_code in master_dict:
                m_rec = master_dict[e_code]

                old_jcn = m_rec.get("jcn")
                new_jcn = ed_rec.jcn
                if old_jcn and new_jcn and str(old_jcn) != str(new_jcn):
                    logger.warning(
                        f"⚠️ JCN変更検知: {e_code} ({ed_rec.submitter_name}) 旧JCN={old_jcn} → 新JCN={new_jcn}"
                    )

                old_is_active = bool(m_rec.get("is_active", False))
                sec_code = ed_rec.sec_code or m_rec.get("code")

                if sec_code:
                    if old_is_active is False and is_listed_official is True:
                        listing_events.append({"code": sec_code, "type": "LISTING", "event_date": today})
                        logger.info(f"🟢 新規上場/再上場検知: {sec_code} ({ed_rec.submitter_name})")
                    elif old_is_active is True and is_listed_official is False:
                        listing_events.append({"code": sec_code, "type": "DELISTING", "event_date": today})
                        logger.info(f"🔴 上場廃止検知: {sec_code} ({ed_rec.submitter_name})")

                updates = {
                    "jcn": ed_rec.jcn or m_rec.get("jcn"),
                    "code": sec_code,
                    "company_name": ed_rec.submitter_name,
                    "company_name_en": ed_rec.submitter_name_en,
                    "submitter_name_kana": ed_rec.submitter_name_kana,
                    "submitter_type": ed_rec.submitter_type,
                    "is_consolidated": ed_rec.is_consolidated,
                    "capital": ed_rec.capital,
                    "settlement_date": ed_rec.settlement_date,
                    "address": ed_rec.address,
                    "industry_edinet": ed_rec.industry_edinet,
                    "industry_edinet_en": ed_rec.industry_edinet_en,
                    "is_listed_edinet": is_listed_official,
                    "is_active": is_listed_official,  # 【修正】古い値を維持せず、金融庁の事実(FACT)を反映させる
                }

                changed = False
                for k, v in updates.items():
                    if m_rec.get(k) != v:
                        m_rec[k] = v
                        changed = True

                if changed:
                    master_dict[e_code] = m_rec
                    updated_count += 1
            else:
                sec_code = ed_rec.sec_code

                if sec_code and is_listed_official:
                    listing_events.append({"code": sec_code, "type": "LISTING", "event_date": today})

                new_master_rec = StockMasterRecord(
                    edinet_code=e_code,
                    code=sec_code,
                    jcn=ed_rec.jcn,
                    company_name=ed_rec.submitter_name,
                    company_name_en=ed_rec.submitter_name_en,
                    submitter_name_kana=ed_rec.submitter_name_kana,
                    submitter_type=ed_rec.submitter_type,
                    is_consolidated=ed_rec.is_consolidated,
                    capital=ed_rec.capital,
                    settlement_date=ed_rec.settlement_date,
                    address=ed_rec.address,
                    industry_edinet=ed_rec.industry_edinet,
                    industry_edinet_en=ed_rec.industry_edinet_en,
                    is_listed_edinet=is_listed_official,
                    is_active=is_listed_official,
                )
                master_dict[e_code] = new_master_rec.model_dump()
                updated_count += 1

        aggregation_applied_count = 0
        for old_code, new_code in self.cm.aggregation_map.items():
            old_info = self.cm.edinet_codes.get(old_code)
            if old_info:
                old_name = old_info.submitter_name
                old_sec = old_info.sec_code
            elif old_code in master_dict:
                m_old = master_dict[old_code]
                old_name = m_old.get("company_name", "不明")
                old_sec = m_old.get("code")
            else:
                old_name = "不明"
                old_sec = None

            old_sec_disp = f"証券コード:{old_sec}" if old_sec else "コードなし"

            if new_code in master_dict:
                m_rec = master_dict[new_code]
                new_name = m_rec.get("company_name", "不明")
                new_sec = m_rec.get("code")
                new_sec_disp = f"証券コード:{new_sec}" if new_sec else "非上場"

                existing_former = m_rec.get("former_edinet_codes") or ""
                former_set = set(existing_former.split(",")) if existing_former else set()
                if old_code not in former_set:
                    former_set.add(old_code)
                    former_set.discard("")
                    m_rec["former_edinet_codes"] = ",".join(sorted(former_set))
                    aggregation_applied_count += 1
                    logger.debug(
                        f"集約ブリッジ適用: {old_code}({old_name} / {old_sec_disp}) → "
                        f"{new_code}({new_name} / {new_sec_disp}) [旧コードをリンク]"
                    )
            else:
                logger.debug(
                    f"集約ブリッジ・スキップ: {old_code}({old_name} / {old_sec_disp}) → {new_code} "
                    f"(継続先 {new_code} が現在の EDINET リストに存在しません)"
                )

        if updated_count > 0 or aggregation_applied_count > 0 or not self.cm.master_df.empty:
            new_df = pd.DataFrame(list(master_dict.values()))

            self.cm.master_df = self.cm._clean_dataframe("master", new_df)

            if (
                updated_count > 0
                or aggregation_applied_count > 0
                or len(self.cm.master_df) != len(pd.DataFrame(list(master_dict.values())))
            ):
                logger.success(
                    f"マスタ同期完了: {updated_count} 件のレコードを更新/追加し、スコープ強制を適用しました。"
                )
                self.cm.hf.save_and_upload("master", self.cm.master_df, clean_fn=self.cm._clean_dataframe, defer=True)

        if listing_events:
            events_df = pd.DataFrame(listing_events).drop_duplicates(subset=["code", "type"])
            self.cm.update_listing_history(events_df)
            logger.success(f"上場履歴同期完了: {len(events_df)} 件のイベントを追加予約しました。")

        master_df = self.cm.master_df
        listed_mask = master_df["is_listed_edinet"].fillna(False).astype(bool)
        has_code_mask = (master_df["code"].notna()) & (master_df["code"] != "")
        is_agg_target_mask = master_df["edinet_code"].isin(self.cm.aggregation_map.values())

        pure_listed = master_df[listed_mask & has_code_mask]
        unlisted_with_code = master_df[~listed_mask & has_code_mask]
        agg_targets_only = master_df[~listed_mask & ~has_code_mask & is_agg_target_mask]

        unique_sec_codes = master_df[has_code_mask]["code"].nunique()
        total_aggregated = master_df["former_edinet_codes"].dropna().str.split(",").str.len().sum()

        logger.success(
            f"同期完了: 総エンティティ数 {len(master_df)} (上場:{len(pure_listed)} / "
            f"コード保持非上場:{len(unlisted_with_code)} / 集約先保護:{len(agg_targets_only)})"
        )
        logger.success(
            f"有効証券コード数 {unique_sec_codes} "
            f"(集約適用: 今回+{aggregation_applied_count}件 / 総保持 {int(total_aggregated)}件)"
        )

    def update_stocks_master(self, incoming_data: pd.DataFrame):
        """
        マスタ更新 & 時系列リコンシリエーション
        レコード追加・属性継承・IPO動的発見などを処理する
        """
        if incoming_data.empty:
            return True

        def resolve_attr(group, col):
            vals = group[col].dropna()
            return vals.iloc[0] if not vals.empty else None

        is_jpx_update = "sector_jpx_33" in incoming_data.columns
        if is_jpx_update:
            jpx_count = len(incoming_data)
            existing_codes = set(self.cm.master_df["code"].dropna().unique())
            incoming_codes = set(incoming_data["code"].dropna().unique())
            new_codes_count = len(incoming_codes - existing_codes)

            logger.info(
                f"📊 JPX マスタ情報注入: 合計 {jpx_count} 件 "
                f"(新規発見: {new_codes_count} 件 / 属性更新: {jpx_count - new_codes_count} 件)"
            )

            # --- 【極限監査修正】JPX リスト外銘柄の非活性化ロジック ---
            # 特殊銘柄（ETF/REIT/PRO）および優先株は、JPX リストに存在しない＝上場廃止/償還と見なす
            master_df = self.cm.master_df
            if not master_df.empty:
                # 現行マスタの特殊銘柄かつアクティブなもの
                def is_jpx_managed(row):
                    market = str(row.get("market") or "").upper()
                    is_special = any(x in market for x in self.SPECIAL_MARKET_KEYWORDS)
                    is_preferred = str(row.get("code") or "")[-1:] != "0"
                    return is_special or is_preferred

                jpx_managed_mask = master_df.apply(is_jpx_managed, axis=1)
                active_mask = master_df["is_active"].fillna(False).astype(bool)

                # リストから消えた銘柄を抽出
                missing_from_jpx = master_df[jpx_managed_mask & active_mask & ~master_df["code"].isin(incoming_codes)]

                if not missing_from_jpx.empty:
                    logger.warning(f"🔴 JPX リスト外銘柄を検知 (非活性化処理): {len(missing_from_jpx)} 件")
                    deactivated_list = []
                    for _, m_row in missing_from_jpx.iterrows():
                        deactivated_rec = m_row.to_dict()
                        deactivated_rec["is_active"] = False
                        deactivated_list.append(deactivated_rec)

                    # incoming_data に偽装して追加処理に回す
                    incoming_data = pd.concat([incoming_data, pd.DataFrame(deactivated_list)], ignore_index=True)

            # 全ての incoming_data に対して、明示的に指定がない場合は Active と見なす
            # (これを行わないと、concat後の NaN が None になり Pydantic でバリデーションエラーになる可能性がある)
            if "is_active" not in incoming_data.columns:
                incoming_data["is_active"] = True
            else:
                incoming_data["is_active"] = incoming_data["is_active"].fillna(True)

        processed_records = []
        for _, row in incoming_data.iterrows():
            rec = row.to_dict()
            rec = {k: (v if not pd.isna(v) else None) for k, v in rec.items()}
            sec_code = rec.get("code")

            if sec_code:
                sec_code = normalize_code(sec_code, nationality="JP")
                rec["code"] = sec_code

                # 優先株判定と親コード設定 (JP:12341 -> JP:12340)
                if ":" in sec_code:
                    prefix, core = sec_code.split(":", 1)
                    if core[-1] != "0":
                        rec["parent_code"] = f"{prefix}:{core[:4]}0"

                if not rec.get("edinet_code") or not rec.get("jcn"):
                    market = str(rec.get("market") or "").upper()
                    is_special = any(x in market for x in self.SPECIAL_MARKET_KEYWORDS)
                    is_preferred = str(sec_code)[-1] != "0"

                    if not self.cm.master_df.empty:
                        # プレフィックス耐性のある検索
                        m_row = self.cm.master_df[self.cm.master_df["code"] == sec_code]
                        if m_row.empty and ":" in str(sec_code):
                            core = str(sec_code).split(":", 1)[1]
                            m_row = self.cm.master_df[self.cm.master_df["code"] == core]
                        
                        if not m_row.empty:
                            m_rec = m_row.iloc[0].to_dict()
                            for k, v in m_rec.items():
                                if k not in rec or rec[k] is None:
                                    rec[k] = v

                    if (not rec.get("edinet_code") or not rec.get("jcn")) and not is_special and not is_preferred:
                        discovery = self.discover_edinet_code(sec_code, name=rec.get("company_name"))
                        if discovery:
                            rec["edinet_code"], rec["jcn"] = discovery
                        else:
                            if not self.cm.master_df.empty and sec_code in self.cm.master_df["code"].values:
                                pass
                            else:
                                logger.debug(
                                    f"Registration Guard: {sec_code} ({rec.get('company_name')}) は"
                                    "EDINET情報が未発見のため、新規登録を保留します。"
                                )
                                continue

            try:
                processed_records.append(StockMasterRecord(**rec).model_dump())
            except Exception as e:
                logger.error(f"銘柄情報のバリデーション失敗 (code: {sec_code}): {e}")

        if not processed_records:
            return True

        incoming_df = pd.DataFrame(processed_records)

        current_m = self.cm.master_df.copy()
        all_states = pd.concat([current_m, incoming_df], ignore_index=True)

        all_states.drop_duplicates(
            subset=["code", "company_name", "last_submitted_at", "is_active", "sector_jpx_33", "market"], inplace=True
        )

        best_records = []
        listing_events = []
        today = pd.Timestamp.now().strftime("%Y-%m-%d")

        # 【Structural Reform】名寄せの主キーを「国籍付証券コード」に統一し、属性のアイソレーションを解消する。
        # code が存在しない（非上場）場合は edinet_code でグルーピングする。
        all_states["identity_key"] = all_states["code"].fillna(all_states["edinet_code"])
        
        for _, group in all_states.groupby("identity_key", dropna=False):
            code_vals = group["code"].dropna().unique()
            code = code_vals[0] if len(code_vals) > 0 else None

            sorted_group = group.sort_index(ascending=False).sort_values(
                "last_submitted_at", ascending=False, na_position="last"
            )
            latest_rec = sorted_group.iloc[0].copy()

            for attr in [
                "sector_jpx_33",
                "sector_jpx_17",
                "market",
                "jcn",
                "edinet_code",
                "parent_code",
                "former_edinet_codes",
                "company_name_en",
                "submitter_name_kana",
                "submitter_type",
                "address",
                "industry_edinet",
                "industry_edinet_en",
                "capital",
                "settlement_date",
                "is_consolidated",
            ]:
                if attr == "former_edinet_codes":
                    all_formers = set()
                    for v in sorted_group[attr].dropna():
                        s_v = str(v).strip()
                        if s_v:
                            all_formers.update([x.strip() for x in s_v.split(",") if x.strip()])
                    if all_formers:
                        latest_rec[attr] = ",".join(sorted(all_formers))
                else:
                    val = resolve_attr(sorted_group, attr)
                    if val is not None:
                        # JPX 由来の更新では法令上の上場フラグは上書きしない
                        if attr == "is_listed_edinet" and is_jpx_update:
                            continue
                        latest_rec[attr] = val

            if code is None:
                best_records.append(latest_rec)
                continue

            new_active = latest_rec.get("is_active", True)
            if not current_m.empty:
                old_row = current_m[current_m["code"] == code]
                if not old_row.empty:
                    old_active = old_row.iloc[0].get("is_active", True)
                    if old_active and not new_active:
                        listing_events.append({"code": code, "type": "DELISTING", "event_date": today})
                    elif not old_active and new_active:
                        listing_events.append({"code": code, "type": "LISTING", "event_date": today})
                else:
                    if new_active:
                        listing_events.append({"code": code, "type": "LISTING", "event_date": today})
            else:
                if new_active:
                    listing_events.append({"code": code, "type": "LISTING", "event_date": today})

            best_records.append(latest_rec)

        self.cm.master_df = pd.DataFrame(best_records)

        if listing_events:
            self.cm.update_listing_history(pd.DataFrame(listing_events))

        return self.cm.hf.save_and_upload("master", self.cm.master_df, clean_fn=self.cm._clean_dataframe, defer=True)

    def reconstruct_name_history(self, code: str) -> pd.DataFrame:
        """
        特定の銘柄について、全書類から社名の変遷（漢字・カナ・英語）を再構成する。
        【工学的主権】取得順に依存せず、常に日付順の隣接比較で境界を検知する。
        """
        catalog_df = self.cm.catalog_df
        if catalog_df.empty:
            return pd.DataFrame()

        # 該当コードの書類を日付順に抽出
        docs = catalog_df[catalog_df["code"] == code].sort_values("submit_at").copy()
        if len(docs) < 2:
            return pd.DataFrame()

        # 現在のマスタ情報を取得 (カナ・英語名の補完用)
        master_info = {}
        if not self.cm.master_df.empty:
            m_row = self.cm.master_df[self.cm.master_df["code"] == code]
            if m_row.empty and ":" in str(code):
                core = str(code).split(":", 1)[1]
                m_row = self.cm.master_df[self.cm.master_df["code"] == core]
            
            if not m_row.empty:
                master_info = m_row.iloc[0].to_dict()

        events = []
        prev_row = docs.iloc[0]

        for _, row in docs.iloc[1:].iterrows():
            curr_name = str(row.get("company_name") or row.get("filerName") or "").strip()
            prev_name = str(prev_row.get("company_name") or prev_row.get("filerName") or "").strip()

            # 正規化して比較 (株式会社 などの揺れを排除)
            if self.normalize_company_name(curr_name) != self.normalize_company_name(prev_name):
                # 基本イベント
                event = {
                    "code": code,
                    "old_name": prev_name,
                    "new_name": curr_name,
                    "change_date": str(row["submit_at"])[:10],
                    "old_name_kana": None,
                    "new_name_kana": master_info.get("submitter_name_kana")
                    if curr_name == master_info.get("company_name")
                    else None,
                    "old_name_en": None,
                    "new_name_en": master_info.get("company_name_en")
                    if curr_name == master_info.get("company_name")
                    else None,
                }

                # 過去のイベントがあればカナ・英語を引き継ぐ
                if events:
                    last_event = events[-1]
                    if last_event["new_name"] == prev_name:
                        event["old_name_kana"] = last_event["new_name_kana"]
                        event["old_name_en"] = last_event["new_name_en"]

                events.append(event)
                prev_row = row

        return pd.DataFrame(events)
