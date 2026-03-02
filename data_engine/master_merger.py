import time
from pathlib import Path

import pandas as pd
from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.utils import HfHubHTTPError
from loguru import logger


class MasterMerger:
    def __init__(self, hf_repo: str, hf_token: str, data_path: Path):
        self.hf_repo = hf_repo
        self.hf_token = hf_token
        self.data_path = data_path
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.api = HfApi() if hf_repo and hf_token else None

    def merge_and_upload(
        self,
        sector: str,
        master_type: str,
        new_data: pd.DataFrame,
        worker_mode: bool = False,
        catalog_manager=None,
        run_id: str = None,
        chunk_id: str = None,
        defer: bool = False,
    ) -> bool:
        """業種別にParquetをロード・結合・アップロード"""
        if new_data.empty:
            return True

        if worker_mode:
            # 【統一】JCN 主導の不変ビン分割 (通常モードと同一ロジック)
            row0 = new_data.iloc[0]
            jcn_val = str(row0.get("jcn", ""))
            if jcn_val and len(jcn_val) >= 2 and jcn_val != "None":
                bin_val = f"J{jcn_val[-2:]}"
            else:
                e_code = str(row0.get("edinet_code", ""))
                if e_code and len(e_code) >= 2 and e_code != "None":
                    bin_val = f"E{e_code[-2:]}"
                else:
                    bin_val = "No"
            filename = f"{master_type}_bin{bin_val}.parquet"

            return catalog_manager.save_delta(
                key="master",
                df=new_data,
                run_id=run_id,
                chunk_id=chunk_id,
                custom_filename=filename,
                defer=defer,
                local_only=True,
            )

        # 物理パスの構成を bin=XX に変更
        # 【極限刷新】JCN（法人番号）を主軸とした完全統一ビン分割 (Unified Binning)
        # 証券コードは変動・欠落の可能性があるため、国家IDである JCN の末尾を分散キーに使用。
        # JCNがない場合のみ、不変の EDINETコード末尾を使用。
        row0 = new_data.iloc[0]
        jcn_val = str(row0.get("jcn", ""))
        if jcn_val and len(jcn_val) >= 2 and jcn_val != "None":
            bin_val = f"J{jcn_val[-2:]}"  # JCN末尾2桁 (e.g., J01)
        else:
            e_code = str(row0.get("edinet_code", ""))
            if e_code and len(e_code) >= 2 and e_code != "None":
                bin_val = f"E{e_code[-2:]}"  # EDINETコード末尾2桁 (e.g., E01)
            else:
                bin_val = "No"

        repo_path = f"master/{master_type}/bin={bin_val}/data.parquet"

        # 1. 既存データのロード
        try:
            m_path = hf_hub_download(repo_id=self.hf_repo, filename=repo_path, repo_type="dataset", token=self.hf_token)
            master_df = pd.read_parquet(m_path)
            logger.debug(f"既存Master読み込み: bin={bin_val} ({len(master_df)} rows)")
            combined_df = pd.concat([master_df, new_data], ignore_index=True)
        except Exception:
            logger.info(f"新規Master作成: bin={bin_val} ({master_type})")
            combined_df = new_data

        # 2. 重複排除 (最新優先)
        subset = ["docid", "key", "context_ref"] if master_type == "financial_values" else ["docid", "key"]

        if "submitDateTime" in combined_df.columns:
            combined_df = combined_df.sort_values("submitDateTime", ascending=False)

        combined_df = combined_df.drop_duplicates(subset=subset, keep="first")

        # 3. 保存とアップロード
        local_file = self.data_path / f"master_bin{bin_val}_{master_type}.parquet"

        for col in combined_df.columns:
            if combined_df[col].dtype == "object":
                # ブール値が含まれる場合は文字列化を避ける (None/NaN が混じっていても型を守る)
                # s.apply(...) よりも s.isin([True, False]) の方が堅牢
                has_bool = combined_df[col].isin([True, False]).any()
                if not has_bool:
                    combined_df[col] = combined_df[col].astype(str)
                else:
                    # 【世界標準】論理値型（Boolean）を維持
                    # 表示ツールによっては true/false となるが、データ解析上の「整合性」「速度」を最優先する。
                    # None を含む Nullable Boolean として保存。
                    pass

        local_file.parent.mkdir(parents=True, exist_ok=True)
        # 【Phase 3 注記】Bin ファイルは XBRL 由来の動的カラム構成のため、
        # 固定 PyArrow スキーマの適用は不適切。dto 推論に委ねるのが正しい設計判断。
        combined_df.to_parquet(local_file, compression="zstd", index=False)

        if self.api:
            # 【重要】defer=True の場合は、モードに関わらずコミットバッファに積む
            if defer and catalog_manager:
                catalog_manager.add_commit_operation(repo_path, local_file)
                logger.debug(f"Master更新をバッファに追加: bin={bin_val} ({master_type})")
                return True

            max_retries = 5  # 3回から5回に強化
            for attempt in range(max_retries):
                try:
                    self.api.upload_file(
                        path_or_fileobj=str(local_file),
                        path_in_repo=repo_path,
                        repo_id=self.hf_repo,
                        repo_type="dataset",
                        token=self.hf_token,
                    )
                    logger.success(f"Master更新成功: bin={bin_val} ({master_type})")
                    return True
                except Exception as e:
                    if isinstance(e, HfHubHTTPError) and e.response.status_code == 429:
                        wait_time = int(e.response.headers.get("Retry-After", 60)) + 5
                        logger.warning(f"Master Rate limit exceeded. Waiting {wait_time}s... ({attempt + 1}/5)")
                        time.sleep(wait_time)
                        continue

                    # 5xx エラー等もリトライ対象に追加
                    if isinstance(e, HfHubHTTPError) and e.response.status_code >= 500:
                        wait_time = 15 * (attempt + 1)
                        logger.warning(
                            f"Master HF Server Error ({e.response.status_code}). "
                            f"Waiting {wait_time}s... ({attempt + 1}/5)"
                        )
                        time.sleep(wait_time)
                        continue

                    logger.error(f"Masterアップロード失敗: bin={bin_val} - {e}")
                    return False
            return False
        return True
