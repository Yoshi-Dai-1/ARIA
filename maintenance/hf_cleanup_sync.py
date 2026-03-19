import pandas as pd
from data_engine.catalog_manager import CatalogManager
from data_engine.core.utils import normalize_code
import sys

def hf_cleanup_sync():
    print("--- ARIA Hugging Face Catalog Sync & Cleanup ---")
    
    # 1. CatalogManager の初期化 (HFから自動ダウンロード)
    # force_refresh=True で最新状態を強制取得
    try:
        cm = CatalogManager(force_refresh=True)
    except Exception as e:
        print(f"Failed to initialize CatalogManager: {e}")
        return

    df = cm.catalog_df
    if df is None or df.empty:
        print("Catalog is empty. Nothing to clean.")
        return

    initial_len = len(df)
    print(f"Initial catalog size: {initial_len} records")

    # 2. クリーンアップロジック (工学的主権)
    # A. JP: 問題の解消 (NULL化)
    jp_mask = df['code'] == 'JP:'
    jp_count = jp_mask.sum()
    if jp_count > 0:
        print(f"[*] Normalizing {jp_count} records with code='JP:' to None...")
        df.loc[jp_mask, 'code'] = None

    # B. メタデータ不全（不開示書類）の削除
    # submit_at, company_name (filerName), doc_type のいずれかが欠落しているものを排除
    invalid_mask = (
        df['submit_at'].isna() | (df['submit_at'] == '') |
        df['company_name'].isna() | (df['company_name'] == '') |
        (df['company_name'] == 'Unknown') |
        df['doc_type'].isna() | (df['doc_type'] == '')
    )
    invalid_count = invalid_mask.sum()
    if invalid_count > 0:
        print(f"[*] Removing {invalid_count} incomplete/ghost records...")
        df = df[~invalid_mask]

    # 3. 反映とアップロード
    if len(df) != initial_len or jp_count > 0:
        print(f"Final catalog size: {len(df)} records")
        cm.catalog_df = df
        
        # HfStorage 経由でアップロードバッファに追加
        cm.hf.save_and_upload("catalog", df, defer=True)
        
        print("Pushing cleaned catalog to Hugging Face...")
        success = cm.push_commit("MAINTENANCE: Cleanup ghost records and normalize codes (Engineering Sovereignty)")
        if success:
            print("[SUCCESS] Catalog on Hugging Face has been synchronized and cleaned.")
        else:
            print("[ERROR] Failed to push commit to Hugging Face.")
    else:
        print("[INFO] Hugging Face catalog is already clean.")

if __name__ == "__main__":
    hf_cleanup_sync()
