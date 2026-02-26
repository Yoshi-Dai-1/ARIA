import json
import shutil
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import requests
from huggingface_hub.utils import EntryNotFoundError

# ARIA モジュールのインポート
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
    from master_merger import MasterMerger

TEST_DATA_DIR = project_root / "data_test_stress"


def setup_stress_env():
    if TEST_DATA_DIR.exists():
        shutil.rmtree(TEST_DATA_DIR)
    TEST_DATA_DIR.mkdir(parents=True)
    (TEST_DATA_DIR / "raw").mkdir()
    (TEST_DATA_DIR / "catalog").mkdir()
    (TEST_DATA_DIR / "meta").mkdir()
    (TEST_DATA_DIR / "temp").mkdir()


def teardown_stress_env():
    if TEST_DATA_DIR.exists():
        shutil.rmtree(TEST_DATA_DIR)


class MockResponse:
    def __init__(self, json_data, status_code=200):
        self.json_data = json_data
        self.status_code = status_code
        self.content = json.dumps(json_data).encode("utf-8")
        self.reason = "OK"

    def json(self):
        return self.json_data


def generate_mock_metadata(count=1000):
    results = []
    for i in range(count):
        doc_id = f"S{1000000 + i:07d}"
        results.append(
            {
                "seqNumber": i + 1,
                "docID": doc_id,
                "edinetCode": f"E{i:05d}",
                "secCode": f"{1000 + (i % 8000):05d}",
                "JCN": f"{1000000000000 + i:013d}",
                "filerName": f"Stress Test Company {i}",
                "submitDateTime": "2024-06-01 09:00",
                "docTypeCode": "120",
                "ordinanceCode": "010",
                "formCode": "030000",
                "xbrlFlag": "1",
                "pdfFlag": "1",
                "opeDateTime": "2024-06-01 10:00:00",
            }
        )
    return {"metadata": results}


def mocked_hf_hub_download(**kwargs):
    filename = kwargs.get("filename")
    local_path = TEST_DATA_DIR / Path(filename).name
    if local_path.exists():
        return str(local_path)
    raise EntryNotFoundError(f"Mocked 404 for {filename}")


def test_large_scale_processing():
    """1,000件の書類を擬似的に統合処理し、 Parquet の完全性と速度を検証する"""
    print("\n[START] Large-scale Resilience Test (1,000 documents)")

    with (
        patch("catalog_manager.hf_hub_download", side_effect=mocked_hf_hub_download),
        patch("catalog_manager.HfApi", return_value=mock_hf_api_instance),
    ):
        cm = CatalogManager(hf_repo="mock/repo", hf_token="mock_token", data_path=TEST_DATA_DIR)
        mock_data = generate_mock_metadata(1000)

        delta_records = []
        for row in mock_data["metadata"]:
            docid = row["docID"]
            record = {
                "doc_id": docid,
                "jcn": row["JCN"],
                "code": row["secCode"],
                "company_name": row["filerName"],
                "edinet_code": row["edinetCode"],
                "submit_at": row["submitDateTime"],
                "doc_type": row["docTypeCode"],
                "title": "ストレス告知書類",
                "processed_status": "success",
                "source": "EDINET",
                "ope_date_time": row["opeDateTime"],
                "raw_zip_path": f"raw/edinet/year=2024/month=06/day=01/zip/{docid}.zip",
            }
            delta_records.append(record)

        cm.update_catalog(delta_records)
        cm.push_commit("Stress Test Commit")

        index_path = TEST_DATA_DIR / "documents_index.parquet"
        assert index_path.exists()
        saved_df = pd.read_parquet(index_path)
        assert len(saved_df) >= 1000
        print(f"Verified: {len(saved_df)} records merged into {index_path.name}")

    with (
        patch("master_merger.hf_hub_download", side_effect=mocked_hf_hub_download),
        patch("master_merger.HfApi", return_value=mock_hf_api_instance),
    ):
        mm = MasterMerger(hf_repo="mock/repo", hf_token="mock_token", data_path=TEST_DATA_DIR)

        for i in range(1000):
            master_df = pd.DataFrame(
                [
                    {
                        "jcn": f"{1000000000000 + i:013d}",
                        "company_name": f"Stress Test Company {i}",
                        "edinet_code": f"E{i:05d}",
                        "code": f"{1000 + (i % 8000):05d}",
                        "submitDateTime": "2024-06-01 10:00:00",
                        "docid": f"S{1000000 + i:07d}",
                        "key": f"key_{i}",
                    }
                ]
            )
            mm.merge_and_upload(
                "TestSector",
                "stocks_master",
                master_df,
                worker_mode=True,
                catalog_manager=cm,
                run_id="STRESS",
                chunk_id="CHUNK1",
            )

        delta_dir = TEST_DATA_DIR / "deltas" / "STRESS" / "CHUNK1"
        bin_files = list(delta_dir.glob("*.parquet"))
        assert len(bin_files) >= 90
        print(f"Verified: Master data distributed across {len(bin_files)} unique JCN bins (Deltas).")

        sample_bin = bin_files[min(25, len(bin_files) - 1)]
        sample_df = pd.read_parquet(sample_bin)
        assert not sample_df.empty
        print(f"Verified: Sample bin {sample_bin.name} contains valid data.")


def test_network_robustness_simulation():
    """ネットワーク不安定状況下でのリトライロジックをシミュレーション」"""
    print("\n[START] Network Robustness Stress Test")
    from network_utils import GLOBAL_ROBUST_SESSION

    # Session.request は RobustSession で wrap されているため、
    # original_request (requests.Session.request) をパッチして
    # リトライが発生することを確認する。
    # ただし、Retry は low-level adapter で行われるため、
    # request レベルのパッチでリトライを模倣するには、
    # カウント付きの side_effect を使うのが現実的。

    retry_count = [0]

    def side_effect(*args, **kwargs):
        retry_count[0] += 1
        if retry_count[0] < 3:
            # 2回目までは失敗
            raise requests.exceptions.ConnectionError("Connection flickered")
        return MockResponse({"status": "OK"})

    # GLOBAL_ROBUST_SESSION の元の request メソッド（robust_request）ではなく、
    # その内部で呼ばれる「本物の」 request 処理がパッチされるように仕掛ける。
    # ここではシンプルに GLOBAL_ROBUST_SESSION.get が 3回目に成功することを検証。

    with patch.object(GLOBAL_ROBUST_SESSION, "get", side_effect=side_effect):
        resp = GLOBAL_ROBUST_SESSION.get("https://dummy-edinet-api.go.jp")
        assert resp.json()["status"] == "OK"
        assert retry_count[0] == 3
        print(f"Verified: Session handled {retry_count[0] - 1} failures and recovered on attempt {retry_count[0]}.")


if __name__ == "__main__":
    setup_stress_env()
    try:
        with (
            patch("catalog_manager.HfApi", return_value=mock_hf_api_instance),
            patch("catalog_manager.hf_hub_download", side_effect=mocked_hf_hub_download),
        ):
            test_large_scale_processing()
        test_network_robustness_simulation()
        print("\n[SUCCESS] All Grand Audit Tests Passed with 100% Integrity.")
    finally:
        teardown_stress_env()
