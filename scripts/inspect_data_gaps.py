import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.resolve()
sys.path.append(str(project_root / "data_engine"))

from market_engine import MarketDataEngine


def inspect_jpx_edinet_gaps():
    print("\n[START] Inspecting JPX Master for EDINET Code Gaps")

    engine = MarketDataEngine(data_path=project_root / "temp_inspection")

    try:
        # 1. JPXから最新の全銘柄マスタを取得
        print("Fetching JPX master data...")
        jpx_master = engine.fetch_jpx_master()

        # 2. 金融庁 EDINET コードリストと照合（通常は CatalogManager が行うが、ここでは手動で傾向を見る）
        # 仮に「市場区分」が ETF や REIT の場合の挙動を確認
        print(f"Total records in JPX master: {len(jpx_master)}")

        # 市場区分別の集計
        market_counts = jpx_master["market"].value_counts()
        print("\n--- Market Distribution in JPX Master ---")
        print(market_counts)

        # 特徴的な銘柄（ETF/ETN/REIT/PRO Market）の抽出
        # これらは証券コードはあるが、EDINETの企業開示（有価証券報告書）の枠組みとは異なる場合が多い
        special_entities = jpx_master[
            jpx_master["market"].str.contains("ETF|ETN|REIT|PRO|証券投資法人", case=False, na=False)
        ]
        print(f"\nSpecial Entities (Potential EDINET-less): {len(special_entities)}")
        print(special_entities.head(10))

        # 実際の運用での「edinet_code 欠落」の根本原因は：
        # 「JPXの銘柄一覧には載っているが、金融庁の『EDINETコードリスト』のエクセルにはまだ（または永久に）載っていない」ケースです。
        # 例：
        # - 新規上場(IPO)直後（JPXにはすぐ載るが、金融庁のコードリスト更新は月数回）
        # - ETF / ETN（EDINETコードはあるが、企業用のリストには載っていないケースや、JCNがないケース）
        # - 外国株

    finally:
        if Path(project_root / "temp_inspection").exists():
            import shutil

            shutil.rmtree(project_root / "temp_inspection")


if __name__ == "__main__":
    inspect_jpx_edinet_gaps()
