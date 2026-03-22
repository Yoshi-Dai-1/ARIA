from data_engine.engines.worker_engine import parse_worker
from data_engine.engines.parsing.edinet.link_base_file_analyzer import account_list_common
from data_engine.catalog_manager import CatalogManager
from data_engine.engines.edinet_engine import EdinetEngine
from pathlib import Path
import os
import pandas as pd

def verify_worker(docid):
    cm = CatalogManager()
    df_cat = pd.read_parquet('data/catalog/documents_index.parquet')
    row = df_cat[df_cat['doc_id'] == docid].iloc[0]
    date_str = row['submit_at'].split(' ')[0]
    year, month, day = date_str.split('-')
    raw_dir = Path("data/1_raw/edinet") / f"year={year}" / f"month={month.zfill(2)}" / f"day={day.zfill(2)}"
    raw_dir.mkdir(parents=True, exist_ok=True)
    zip_path = raw_dir / f"{docid}.zip"
    
    if not zip_path.exists():
        print(f"Downloading {docid}...")
        api_key = os.getenv('EDINET_API_KEY')
        edinet = EdinetEngine(api_key=api_key, data_path="data")
        edinet.download_doc(docid, zip_path)
        
    print(f"\n--- Running worker for {docid} ---")
    acc_obj = account_list_common(data_path=Path("data/meta/taxonomy"), account_list_year="2016")
    
    args = (docid, row.to_dict(), acc_obj, zip_path)
    # Patch logger globally to print INFO nicely since we aren't starting it through main
    import sys
    from loguru import logger
    logger.remove()
    logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level="INFO")
    
    res = parse_worker(args)
    ret_docid, ret_df, ret_err, ret_std = res
    if ret_err:
        print(f"FAILED: {ret_err}")
    else:
        print(f"SUCCESS: Extracted {len(ret_df)} rows")

if __name__ == "__main__":
    for d in ["S1007AG5", "S1007CO8", "S10077LD"]:
        verify_worker(d)
