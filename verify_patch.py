import sys
import traceback
from pathlib import Path

# Add project root to path
sys.path.append(str(Path.cwd()))

try:
    from edinet_engine import EdinetEngine
    from edinet_xbrl_prep.edinet_xbrl_prep.link_base_file_analyzer import account_list_common
except ImportError as e:
    print(f"インポートエラー: {e}")
    print(f"sys.path: {sys.path}")
    sys.exit(1)


def verify_patch():
    print("検証を開始します...")
    # create dummy taxonomy urls
    taxonomy_urls = {"2024": "https://example.com/2024.zip", "2023": "https://example.com/2023.zip"}

    try:
        print("EdinetEngine を初期化しています...")
        # APIキーはダミーでも初期化は通るはずですが、エラーになる場合は適宜修正が必要です
        # EdinetEngineの__init__でlogger.infoが呼ばれるため、ログ設定が必要かもしれません
        engine = EdinetEngine("dummy_api_key", Path("data"), taxonomy_urls=taxonomy_urls)
    except Exception as e:
        print(f"EdinetEngine初期化エラー: {e}")
        traceback.print_exc()
        return

    # Check if _download_taxonomy is patched
    current_method = account_list_common._download_taxonomy
    print(f"現在のメソッド名: {current_method.__name__}")

    if current_method.__name__ == "patched_download_taxonomy":
        print("成功: メソッドは正しくパッチされています。")
    else:
        print("失敗: メソッドがパッチされていません。")
        return

    # Check if the closure captured the urls correctly
    try:
        if current_method.__closure__:
            cell_contents = [c.cell_contents for c in current_method.__closure__]

            found_urls = False
            for content in cell_contents:
                if content == taxonomy_urls:
                    found_urls = True
                    print(f"成功: キャプチャされたタクソノミURLが一致しました: {content}")
                    break

            if not found_urls:
                print(f"失敗: クロージャ内に期待するタクソノミURLが見つかりません。発見された内容: {cell_contents}")
        else:
            print("失敗: パッチされたメソッドにクロージャがありません。")
    except Exception as e:
        print(f"クロージャ検査エラー: {e}")


if __name__ == "__main__":
    verify_patch()
