import shutil
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

project_root = Path(__file__).parent.parent.resolve()
sys.path.append(str(project_root / "data_engine"))

# グローバルに HfApi をモック
mock_hf_api_class = MagicMock()
mock_hf_api_instance = mock_hf_api_class.return_value
mock_hf_api_instance.upload_file.return_value = True
mock_hf_api_instance.create_commit.return_value = True

with (
    patch("huggingface_hub.HfApi", mock_hf_api_class),
    patch("huggingface_hub.hf_hub_download", side_effect=lambda **k: str(Path(k.get("filename")).name)),
):
    from catalog_manager import CatalogManager


def test_enhanced_reconciliation():
    print("\n[START] Verifying Enhanced Reconciliation & Schema Expansion")

    test_data_dir = project_root / "temp_test_enhanced"
    if test_data_dir.exists():
        shutil.rmtree(test_data_dir)
    test_data_dir.mkdir(parents=True)

    try:
        # 1. Mock JPX data with 'sector_jpx_33' and 'market'
        jpx_data = pd.DataFrame(
            [{"code": "13330", "company_name": "マルハニチロ", "sector_jpx_33": "Fishery", "market": "Prime"}]
        )

        # 2. Mock CatalogManager
        with (
            patch("catalog_manager.HfApi", return_value=mock_hf_api_instance),
            patch("catalog_manager.CatalogManager._load_parquet", return_value=pd.DataFrame()),
        ):
            cm = CatalogManager(hf_repo="mock/repo", hf_token="mock_token", data_path=test_data_dir)

            # 手動で edinet_codes をセットして同期をシミュレート
            from models import EdinetCodeRecord

            cm.edinet_codes = {
                "E00012": EdinetCodeRecord(
                    edinet_code="E00012",
                    submitter_type="内国法人・組合",
                    is_listed="上場",
                    is_consolidated="有",
                    capital=20000000000.0,
                    settlement_date="3月31日",
                    submitter_name="マルハニチロ株式会社",
                    submitter_name_en="Maruha Nichiro Corporation",
                    submitter_name_kana="マルハニチロ",
                    address="東京都江東区豊洲三丁目2番20号",
                    industry_edinet="水産・農林業",
                    sec_code="13330",
                    jcn="1010001111111",
                )
            }

            # 反映実行
            cm._update_master_from_edinet_codes()

            # JPXマスタとのリコンシリエーション
            cm.update_stocks_master(jpx_data)

            master = cm.get_master_df() if hasattr(cm, "get_master_df") else cm.master_df
            rec = master[master["code"] == "13330"].iloc[0].to_dict()

            print("\n--- Verified Record (13330) ---")
            for k, v in rec.items():
                print(f"{k}: {v} (type: {type(v)})")

            # アサーション: 新設カラム
            assert rec["submitter_type"] == "内国法人・組合"
            assert rec["capital"] == 20000000000.0
            assert rec["address"] == "東京都江東区豊洲三丁目2番20号"
            assert rec["company_name_en"] == "Maruha Nichiro Corporation"

            # アサーション: JPX属性の保持
            assert rec["sector_jpx_33"] == "Fishery"
            assert rec["market"] == "Prime"

            # アサーション: NaN汚染がないこと
            # company_name_en が以前は "nan" になっていたが、今回は補完されているはず
            assert rec["company_name_en"] != "nan"

            print("\n[SUCCESS] Enhanced schema verified. No NaN contamination. JPX attributes preserved.")

    finally:
        if test_data_dir.exists():
            shutil.rmtree(test_data_dir)


if __name__ == "__main__":
    test_enhanced_reconciliation()
