from data_engine.engines.parsing.edinet.xbrl_parser_wrapper import get_xbrl_wrapper
import os
import shutil
from pathlib import Path
import pandas as pd

def test_us_gaap(docid, zip_path_str):
    temp_dir = Path(f"tmp_{docid}")
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir()
    
    print(f"\n--- Testing docid: {docid} ---")
    df, log_dict = get_xbrl_wrapper(docid, zip_path_str, temp_dir, temp_dir)
    
    if df is not None and not df.empty:
        df['ns'] = df['key'].apply(lambda x: x.split(':')[0] if pd.notnull(x) and ':' in x else 'unknown')
        print(f"Total entries extracted: {len(df)}")
        print("Namespaces present:")
        print(df['ns'].value_counts())
        
        quant = df[df['isTextBlock_flg'] == 0]
        text = df[df['isTextBlock_flg'] == 1]
        print(f"Quantitative: {len(quant)}")
        print(f"Text Blocks : {len(text)}")
        
        us_tags = df[df['ns'].str.contains('us-gaap', case=False, na=False)]
        print(f"US GAAP exact namespace found: {len(us_tags)}")
        if not us_tags.empty:
            print("US GAAP Examples:")
            print(us_tags[['key', 'element_name', 'data_str']].head(10))
            
        print("\nText Blocks Examples:")
        print(text[['key', 'data_str']].head(3))
        
    else:
        print("Failed or Empty")
        print(log_dict)

if __name__ == "__main__":
    # Kubota S10078D2 (2015)
    test_us_gaap("S10078D2", "data/1_raw/edinet/year=2016/month=03/day=25/S10078D2.zip")
    
    # Canon might not be downloaded, but we can try downloading it
    from data_engine.engines.edinet_engine import EdinetEngine
    api_key = os.getenv('EDINET_API_KEY')
    edinet = EdinetEngine(api_key=api_key, data_path="data")
    canon_docid = "S10079BF"
    df_cat = pd.read_parquet('data/catalog/documents_index.parquet')
    c_row = df_cat[df_cat['doc_id'] == canon_docid].iloc[0]
    y, m, d = c_row['submit_at'].split(' ')[0].split('-')
    c_raw_dir = Path("data/1_raw/edinet") / f"year={y}" / f"month={m.zfill(2)}" / f"day={d.zfill(2)}"
    c_raw_dir.mkdir(parents=True, exist_ok=True)
    c_zip = c_raw_dir / f"{canon_docid}.zip"
    if not c_zip.exists():
        print(f"Downloading Canon {canon_docid}...")
        edinet.download_doc(canon_docid, c_zip)
    test_us_gaap(canon_docid, str(c_zip))
