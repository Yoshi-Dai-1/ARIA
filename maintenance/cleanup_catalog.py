import pandas as pd
from pathlib import Path
import sys

def cleanup_catalog(catalog_path: str):
    """
    ARIA カタログの不整合データを物理的事実に基づき正規化・クリーンアップする。
    """
    path = Path(catalog_path)
    if not path.exists():
        print(f"Error: Catalog not found at {path}")
        return

    print(f"--- ARIA Catalog Cleanup Job ---")
    print(f"Loading: {path.absolute()}")
    
    df = pd.read_parquet(path)
    initial_len = len(df)
    
    # 1. code == 'JP:' の正規化 (NULL化)
    # 以前のバグにより非上場銘柄に付与されていた不当なプレフィックスを排除
    jp_mask = df['code'] == 'JP:'
    jp_count = jp_mask.sum()
    if jp_count > 0:
        print(f"[*] Normalizing {jp_count} records with code='JP:' to NULL...")
        df.loc[jp_mask, 'code'] = None

    # 2. メタデータ不全（不開示書類の残骸）の物理削除
    # 提出日時が特定できないものは時系列データとして成立しないため削除
    # (Discovery Filtering 導入前の過去データが対象)
    invalid_date_mask = df['submit_at'].isna() | (df['submit_at'] == '')
    date_count = invalid_date_mask.sum()
    if date_count > 0:
        print(f"[*] Removing {date_count} records with empty submit_at (Invalid Metadata)...")
        df = df[~invalid_date_mask]

    # 3. Sentinel Value の一貫性チェック (Optional)
    # company_name == 'Unknown' や bin_id == 'No' は、現在は許容バケットとして維持

    # 保存
    if len(df) != initial_len or jp_count > 0:
        print(f"[SUCCESS] Cleanup completed.")
        print(f"  - Initial records: {initial_len}")
        print(f"  - Final records  : {len(df)}")
        print(f"  - Normalized/Deleted: {initial_len - len(df) + jp_count} actions")
        df.to_parquet(path, index=False)
    else:
        print("[INFO] No inconsistencies found. Catalog is healthy.")

if __name__ == "__main__":
    # デフォルトパスまたは引数からの指定
    target = sys.argv[1] if len(sys.argv) > 1 else "/Users/yoshi_dai/repos/ARIA/data/catalog/documents_index.parquet"
    cleanup_catalog(target)
