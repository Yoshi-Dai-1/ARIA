import os
import sys
import pandas as pd
import sqlite3
import shutil
import traceback
import urllib3
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from tqdm import tqdm
from time import sleep
from dotenv import load_dotenv
from pathlib import Path

# 警告の抑制
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. 検索パスにサブモジュールのルートフォルダを追加
submodule_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'edinet_xbrl_prep'))
sys.path.insert(0, submodule_root)

# 2. パッケージ形式でインポートする
from edinet_xbrl_prep.edinet_api import request_term, request_doc, edinet_response_metadata
from edinet_xbrl_prep.link_base_file_analyzer import account_list_common
from edinet_xbrl_prep.fs_tbl import get_fs_tbl

# 環境変数の読み込み
load_dotenv()
API_KEY = os.getenv("EDINET_API_KEY")

if not API_KEY:
    print("エラー: 環境変数 EDINET_API_KEY が設定されていません。")
    exit(1)

# 設定
DATA_PATH = Path("./data")
DATA_PATH.mkdir(exist_ok=True)
RAW_XBRL_DIR = DATA_PATH / "raw/xbrl_doc"
RAW_XBRL_EXT_DIR = DATA_PATH / "raw/xbrl_doc_ext"
RAW_XBRL_DIR.mkdir(parents=True, exist_ok=True)
RAW_XBRL_EXT_DIR.mkdir(parents=True, exist_ok=True)

# 取得期間設定
START_DATE = "2024-06-01"
END_DATE = "2024-06-30"

# 抽出対象のロール設定
FS_DICT = {
    'BS': ["_BalanceSheet", "_ConsolidatedBalanceSheet"],
    'PL': ["_StatementOfIncome", "_ConsolidatedStatementOfIncome"],
    'CF': ["_StatementOfCashFlows", "_ConsolidatedStatementOfCashFlows"],
    'SS': ["_StatementOfChangesInEquity", "_ConsolidatedStatementOfChangesInEquity"],
    'notes': ["_Notes", "_ConsolidatedNotes"],
    'report': ["_CabinetOfficeOrdinanceOnDisclosure"]
}
ALL_ROLES = []
for roles in FS_DICT.values():
    ALL_ROLES.extend(roles)

# GitHub Actions環境ではコア数を最大限活用
PARALLEL_WORKERS = os.cpu_count() or 2
# 並列実行時のバッチサイズ（メモリと相談）
BATCH_PARALLEL_SIZE = 8

def get_db_path(year, sector_label):
    safe_sector = str(sector_label).replace("/", "・").replace("\\", "・")
    year_dir = DATA_PATH / str(year)
    year_dir.mkdir(exist_ok=True)
    return year_dir / f"{year}_{safe_sector}.db"

def init_db(conn):
    cursor = conn.cursor()
    cursor.execute('PRAGMA synchronous = OFF')
    cursor.execute('PRAGMA journal_mode = WAL')
    cursor.execute('PRAGMA cache_size = -100000') # 約100MBのキャッシュ
    cursor.execute('''CREATE TABLE IF NOT EXISTS documents (docID TEXT PRIMARY KEY, secCode TEXT, filerName TEXT, submitDateTime TEXT, docDescription TEXT, periodStart TEXT, periodEnd TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS financial_data (id INTEGER PRIMARY KEY AUTOINCREMENT, docID TEXT, secCode TEXT, filerName TEXT, key TEXT, data_str TEXT, period_start TEXT, period_end TEXT, instant_date TEXT, scenario TEXT, label_jp TEXT, label_en TEXT, unit TEXT, decimals TEXT, UNIQUE(secCode, key, period_start, period_end, instant_date, scenario))''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fin_docid ON financial_data(docID)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fin_seccode ON financial_data(secCode)')
    conn.commit()

def save_sector_batch_to_db(db_path, batch_data):
    if not batch_data:
        return
    
    # print(f" -> DB更新中: {db_path.name} ({len(batch_data)}書類分)")
    conn = sqlite3.connect(db_path)
    init_db(conn)
    cursor = conn.cursor()
    
    try:
        docs = []
        fin_records = []
        for doc_meta, fs_df in batch_data:
            docs.append((doc_meta['docID'], doc_meta['secCode'], doc_meta['filerName'], doc_meta['submitDateTime'], doc_meta['docDescription'], doc_meta['periodStart'], doc_meta['periodEnd']))
            for _, row in fs_df.iterrows():
                fin_records.append((doc_meta['docID'], doc_meta['secCode'], doc_meta['filerName'], row.get('key'), str(row.get('data_str')), row.get('period_start'), row.get('period_end'), row.get('instant_date'), row.get('scenario'), row.get('label_jp'), row.get('label_en'), row.get('unit'), row.get('decimals')))

        cursor.executemany('INSERT OR REPLACE INTO documents (docID, secCode, filerName, submitDateTime, docDescription, periodStart, periodEnd) VALUES (?, ?, ?, ?, ?, ?, ?)', docs)
        cursor.executemany('INSERT OR REPLACE INTO financial_data (docID, secCode, filerName, key, data_str, period_start, period_end, instant_date, scenario, label_jp, label_en, unit, decimals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', fin_records)
        conn.commit()
    except Exception as e:
        print(f"DB保存エラー: {e}")
    finally:
        conn.close()

def parse_worker(task):
    """マルチプロセス用ワーカー: 1書類の解析を実行"""
    docid_str, row_dict, account_list = task
    try:
        zip_path = RAW_XBRL_DIR / f"{docid_str}.zip"
        extract_dir = RAW_XBRL_EXT_DIR / docid_str
        
        fs_df = get_fs_tbl(
            account_list_common_obj=account_list,
            docid=docid_str,
            zip_file_str=str(zip_path),
            temp_path_str=str(extract_dir),
            role_keyward_list=ALL_ROLES
        )
        
        doc_meta = {
            'docID': docid_str, 'secCode': row_dict.get('secCode'), 'filerName': row_dict.get('filerName'),
            'submitDateTime': str(row_dict['submitDateTime']), 'docDescription': row_dict.get('docDescription'),
            'periodStart': row_dict.get('periodStart'), 'periodEnd': row_dict.get('periodEnd')
        }
        
        # 成功後は解析済みフォルダを消す（ZIPは後で親で消す）
        if extract_dir.exists(): shutil.rmtree(extract_dir)
        return docid_str, doc_meta, fs_df, None
    except Exception as e:
        return docid_str, None, None, str(e)

def main():
    print(f"{START_DATE} から {END_DATE} の書類一覧を取得中...")
    res_results = request_term(api_key=API_KEY, start_date_str=START_DATE, end_date_str=END_DATE)
    
    edinet_meta = edinet_response_metadata(tse_sector_url="https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls", tmp_path_str=str(DATA_PATH))
    edinet_meta.set_data(res_results)

    raw_df = edinet_meta.get_metadata_pandas_df()
    if raw_df.empty or 'docTypeCode' not in raw_df.columns:
        print("書類が見つかりませんでした。")
        return

    yuho_df = edinet_meta.get_yuho_df()
    yuho_filtered = yuho_df[yuho_df['docTypeCode'] == '120'].copy()
    if yuho_filtered.empty:
        print("対象書類（有報）がありません。")
        return

    if "docID" in yuho_filtered.columns:
        yuho_filtered.set_index("docID", inplace=True)
    
    yuho_filtered['submitYear'] = yuho_filtered['submitDateTime'].str[:4]
    yuho_filtered = yuho_filtered.sort_values(['submitYear', 'sector_label_33'])
    
    print(f"対象書類数: {len(yuho_filtered)}")
    print("共通タクソノミを準備中...")
    account_list = account_list_common(data_path=DATA_PATH, account_list_year="2024")

    current_db_path = None
    sector_batch = []
    processed_docids = set()

    # 高速化のため、ダウンロード済みリストを作成
    print("未取得の書類をダウンロード中...")
    tasks_to_parse = []
    
    for docid, row in yuho_filtered.iterrows():
        docid_str = str(docid)
        submit_year = row['submitYear']
        sector_label = row.get('sector_label_33', 'その他')
        db_path = get_db_path(submit_year, sector_label)
        
        # 業種が変わった時の処理
        if current_db_path and db_path != current_db_path:
            processed_docids = set()
        current_db_path = db_path

        # 重複チェック用DB問い合わせ（キャッシュ）
        if not processed_docids and db_path.exists():
            conn_check = sqlite3.connect(db_path)
            cursor = conn_check.cursor()
            cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='documents'")
            if cursor.fetchone():
                cursor.execute('SELECT docID FROM documents')
                processed_docids = {str(r[0]) for r in cursor.fetchall()}
            conn_check.close()
        
        if docid_str in processed_docids:
            continue

        # タスクに追加
        tasks_to_parse.append((docid_str, row.to_dict()))

    print(f"実処理対象: {len(tasks_to_parse)} 件 (GitHub Actions並列数: {PARALLEL_WORKERS})")

    # 並列実行エンジン
    with ProcessPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        # バッチ処理（数件ずつダウンロードして並列解析 → 保存）
        for i in range(0, len(tasks_to_parse), BATCH_PARALLEL_SIZE):
            batch_tasks = tasks_to_parse[i:i+BATCH_PARALLEL_SIZE]
            
            # 1. 順次ダウンロード（API負荷考慮、解析中に空き時間を埋めるなら理想は別スレッドだがまずこれで）
            for docid_str, _ in batch_tasks:
                zip_path = RAW_XBRL_DIR / f"{docid_str}.zip"
                if not zip_path.exists():
                    res_doc = request_doc(api_key=API_KEY, docid=docid_str, out_filename_str=str(zip_path))
                    if res_doc.status != "success":
                        if zip_path.exists(): os.remove(zip_path)
                    sleep(0.5)

            # 2. 並列解析
            futures = [executor.submit(parse_worker, (d_id, row, account_list)) for d_id, row in batch_tasks]
            
            results_to_save = {} # db_path -> list of (meta, df)
            
            for future in as_completed(futures):
                d_id, meta, df, err = future.result()
                if err:
                    print(f"エラー ({d_id}, 継続します): {err}")
                    continue
                
                # 保存用データのマッピング
                target_year = meta['submitDateTime'][:4]
                target_sector = batch_tasks[0][1].get('sector_label_33', 'その他') # 近い業種が集まっているはず
                # 厳密にはmetaから引く
                db_p = get_db_path(target_year, meta.get('sector_label_33', 'その他'))
                
                if db_p not in results_to_save:
                    results_to_save[db_p] = []
                results_to_save[db_p].append((meta, df))
                
                # 使い終わったZIPを消す
                z_path = RAW_XBRL_DIR / f"{d_id}.zip"
                if z_path.exists(): os.remove(z_path)

            # 3. 業種ごとに一括保存（書き込みの最大効率化）
            for db_p, data_list in results_to_save.items():
                save_sector_batch_to_db(db_p, data_list)

    print("全ての処理が完了しました。")

if __name__ == "__main__":
    main()
