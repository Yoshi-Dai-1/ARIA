from loguru import logger


class MergerEngine:
    """Mergerモード: デルタファイルの集約とGlobal更新 (Atomic Commit & Rollback 戦略)"""

    def __init__(self, catalog, run_id):
        self.catalog = catalog
        self.merger = catalog.merger
        self.run_id = run_id

    def run(self) -> bool:
        logger.info(f"=== Merger Started (RunID: {self.run_id}) ===")

        # 1. スナップショットの取得 (Rollback用)
        self.catalog.take_snapshot()

        # 2. 全てのデルタファイルを収集
        deltas = self.catalog.load_deltas(self.run_id)
        if not deltas:
            logger.warning(f"処理対象のデルタが見つかりません。RunID: {self.run_id}")
            return True

        # 3. カタログのマージ
        if "catalog" in deltas:
            df_cat = deltas["catalog"]
            logger.info(f"カタログデルタをマージ中: {len(df_cat)} 件")
            self.catalog.update_catalog(df_cat.to_dict("records"))

        # 4. マスタデータ (financial / qualitative) のマージ
        # 業種・Binごとに分割されたデータを統合して MasterMerger に渡す
        for key, df in deltas.items():
            if key == "catalog":
                continue

            try:
                # key 形式: financial_{sector} or text_{sector} or financial_bin{bin}
                if key.startswith("financial_"):
                    m_type = "financial_values"
                    sector_or_bin = key.replace("financial_", "")
                elif key.startswith("text_"):
                    m_type = "qualitative_text"
                    sector_or_bin = key.replace("text_", "")
                else:
                    logger.warning(f"未知のデルタキーをスキップ: {key}")
                    continue

                if sector_or_bin.startswith("bin"):
                    bin_val = sector_or_bin.replace("bin", "")
                    logger.info(f"Master更新 (Bin統合): {m_type} | bin={bin_val}")
                    self.merger.merge_and_upload(
                        bin_val,
                        m_type,
                        df,
                        worker_mode=False,
                        catalog_manager=self.catalog,
                        defer=True,
                    )
                else:
                    logger.info(f"Master更新 (業種統合): {m_type} | sector={sector_or_bin}")
                    self.merger.merge_and_upload(
                        sector_or_bin,
                        m_type,
                        df,
                        worker_mode=False,
                        catalog_manager=self.catalog,
                        defer=True,
                    )
            except Exception as e:
                logger.error(f"Master統合失敗 ({key}): {e}")
                self.catalog.rollback(f"Master Merge Failure: {key}")
                return False

        # 5. アトミックな確定 (Push Commit)
        logger.info("全ての更新を Hugging Face に一括コミットします...")
        success = self.catalog.push_commit(f"Atomic Build: {self.run_id}")

        if success:
            logger.success(f"=== Merger完了: RunID {self.run_id} の全ての更新が確定されました ===")
            # 完了後にデルタを清掃
            self.catalog.cleanup_deltas(self.run_id, cleanup_old=True)
            return True
        else:
            logger.critical("Final Commit Failed! Rolling back to snapshots...")
            self.catalog.rollback(f"Commit Failure: {self.run_id}")
            return False
