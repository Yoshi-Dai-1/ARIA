from data_engine.catalog_manager import CatalogManager
from data_engine.engines.edinet_engine import EdinetEngine
from data_engine.engines.parsing.edinet.fs_tbl import get_fs_tbl
from data_engine.engines.parsing.edinet.link_base_file_analyzer import account_list_common
import os
import shutil
from pathlib import Path
import pandas as pd

def test_us_gaap():
    docid = "S10078D2" # Kubota, periodEnd=2015-12-31 (Submitted probably March 2016)
    
    cm = CatalogManager()
    api_key = os.getenv('EDINET_API_KEY')
    edinet = EdinetEngine(api_key=api_key, data_path="data")
    
    df_cat = pd.read_parquet('data/catalog/documents_index.parquet')
    target_row = df_cat[df_cat['doc_id'] == docid].iloc[0]
    date_str = target_row['submit_at'].split(' ')[0]
    year, month, day = date_str.split('-')
    
    raw_dir = Path("data/1_raw/edinet") / f"year={year}" / f"month={month.zfill(2)}" / f"day={day.zfill(2)}"
    raw_dir.mkdir(parents=True, exist_ok=True)
    zip_path = raw_dir / f"{docid}.zip"
    
    if not zip_path.exists():
        print(f"Downloading {docid}...")
        edinet.download_doc(docid, zip_path)
    
    print(f"File exists: {zip_path.exists()} at {zip_path}")
    
    acc_obj = account_list_common(data_path=Path("data/meta/taxonomy"), account_list_year="2016")
    acc_obj.download_taxonomy_components()
    acc_obj.make_account_common_df()
    
    temp_dir = Path(f"tmp_{docid}")
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir()
    
    # Extract using ZipFile to simulate worker_engine behavior
    from zipfile import ZipFile
    with ZipFile(str(zip_path)) as zf:
        for member in zf.namelist():
            if "PublicDoc" in member or "AuditDoc" in member:
                zf.extract(member, temp_dir)
    
    print("Parsing US GAAP...")
    df = get_fs_tbl(acc_obj, docid, str(zip_path), str(temp_dir), [])
    
    if df is not None and not df.empty:
        quant_only = df[df['isTextBlock_flg'] == 0]
        text_only = df[df['isTextBlock_flg'] == 1]
        print(f"Total entries: {len(df)}")
        print(f"Quantitative (Numeric): {len(quant_only)}")
        print(f"Text Blocks (Qualitative): {len(text_only)}")
        
        # Check namespaces
        print("Keys namespaces (sample):")
        print(df['key'].apply(lambda k: k.split(':')[0] if pd.notnull(k) and ':' in k else 'unknown').value_counts())
        
        # Check Text Blocks length and look at the first few
        print("Text Blocks preview:")
        print(text_only[['key', 'element_name']].head(5))
        
        us_keys = df[df['key'].str.contains('us-gaap', na=False, case=False)]
        print(f"\nUS GAAP prefixed tags found: {len(us_keys)}")
        if not us_keys.empty:
            print(us_keys['key'].head(10).tolist())
    else:
         print("Parsing failed or empty DataFrame returned")

if __name__ == "__main__":
    test_us_gaap()
