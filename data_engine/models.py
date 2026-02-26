import math
from typing import Any, Optional

from pydantic import BaseModel, field_validator


class EdinetDocument(BaseModel):
    """EDINET APIから取得される書類メタデータのバリデーションモデル (API v2 全フィールド網羅)"""

    seqNumber: int
    docID: str
    edinetCode: Optional[str] = None
    secCode: Optional[str] = None
    JCN: Optional[str] = None
    filerName: Optional[str] = None
    fundCode: Optional[str] = None
    ordinanceCode: Optional[str] = None
    formCode: Optional[str] = None
    docTypeCode: Optional[str] = None
    periodStart: Optional[str] = None
    periodEnd: Optional[str] = None
    submitDateTime: str
    docDescription: Optional[str] = None
    issuerEdinetCode: Optional[str] = None
    subjectEdinetCode: Optional[str] = None
    subsidiaryEdinetCode: Optional[str] = None
    currentReportReason: Optional[str] = None
    parentDocID: Optional[str] = None
    opeDateTime: Optional[str] = None
    withdrawalStatus: str = "0"
    docInfoEditStatus: str = "0"
    disclosureStatus: str = "0"
    xbrlFlag: str = "0"
    pdfFlag: str = "0"
    attachDocFlag: str = "0"
    englishDocFlag: str = "0"
    csvFlag: str = "0"
    legalStatus: str = "0"


class EdinetCodeRecord(BaseModel):
    """金融庁公表のEDINETコードリストレコード (13項目 + 英語版補完情報)"""

    edinet_code: str
    submitter_type: Optional[str] = None  # 提出者種別
    is_listed: Optional[str] = None  # 上場区分 (上場/非上場)
    is_consolidated: Optional[str] = None  # 連結の有無
    capital: Optional[float] = None  # 資本金
    settlement_date: Optional[str] = None  # 決算日
    submitter_name: str  # 提出者名 (和文)
    submitter_name_en: Optional[str] = None  # 提出者名 (和文/英字)
    submitter_name_kana: Optional[str] = None  # 提出者名 (ヨミ)
    address: Optional[str] = None  # 所在地
    industry_edinet: Optional[str] = None  # 提出者業種 (和文)
    industry_edinet_en: Optional[str] = None  # 提出者業種 (英文/英語版リストより取得)
    sec_code: Optional[str] = None  # 証券コード (5桁)
    jcn: Optional[str] = None  # 提出者法人番号 (JCN)

    @field_validator(
        "submitter_type",
        "is_listed",
        "is_consolidated",
        "settlement_date",
        "submitter_name_en",
        "submitter_name_kana",
        "address",
        "industry_edinet",
        "industry_edinet_en",
        "sec_code",
        "jcn",
        mode="before",
    )
    @classmethod
    def nan_to_none(cls, v: Any) -> Any:
        if isinstance(v, float) and math.isnan(v):
            return None
        return v


class CatalogRecord(BaseModel):
    """統合ドキュメントカタログ (documents_index.parquet) のレコードモデル (30カラム構成)"""

    # 1. Identifiers (識別子・基本情報)
    doc_id: str
    jcn: Optional[str] = None  # 法人番号 (Japan Corporate Number)
    code: str  # 証券コード (5桁)
    company_name: str
    edinet_code: Optional[str] = None
    issuer_edinet_code: Optional[str] = None  # 発行者EDINETコード
    subject_edinet_code: Optional[str] = None  # 公開買付対象者EDINETコード
    subsidiary_edinet_code: Optional[str] = None  # 子会社EDINETコード (カンマ区切り)
    fund_code: Optional[str] = None  # ファンドコード (投資信託等)

    # 2. Timeline (時間軸)
    submit_at: str

    # 3. Domain/Fiscal (決算・期間属性)
    fiscal_year: Optional[int] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    num_months: Optional[int] = None
    accounting_standard: Optional[str] = None  # 会計基準 (J-GAAP, IFRS, etc.)

    # 4. Document Details (書類詳細特性)
    doc_type: Optional[str] = None
    title: Optional[str] = None
    form_code: Optional[str] = None
    ordinance_code: Optional[str] = None
    is_amendment: bool = False
    parent_doc_id: Optional[str] = None  # 訂正対象の親書類ID
    withdrawal_status: Optional[str] = None  # 取下区分 (1:取下済)
    doc_info_edit_status: Optional[str] = None  # 財務局修正状態 (1:修正情報, 2:修正された書類)
    disclosure_status: Optional[str] = None  # 開示ステータス (1:OK, 2:修正 etc.)
    current_report_reason: Optional[str] = None  # 臨時報告書の提出理由

    # 5. Infrastructure (システム管理情報)
    raw_zip_path: Optional[str] = None
    pdf_path: Optional[str] = None
    processed_status: Optional[str] = "success"
    source: str = "EDINET"

    # 6. API V2 Lifecycle (増分同期・運用メタデータ)
    ope_date_time: Optional[str] = None  # 操作日時 (API V2 の核心項目)


class StockMasterRecord(BaseModel):
    """銘柄マスタ (stocks_master.parquet) のレコードモデル (ARIA統合マスタ)"""

    edinet_code: str  # Primary Key (EDINETシステム上の座席番号)
    code: Optional[str] = None  # 証券コード (5桁、非上場の場合は None)
    jcn: Optional[str] = None  # 法人番号 (物理パスの分散キー)
    company_name: str
    company_name_en: Optional[str] = None
    sector_jpx_33: Optional[str] = None
    sector_jpx_17: Optional[str] = None
    industry_edinet: Optional[str] = None
    industry_edinet_en: Optional[str] = None
    market: Optional[str] = None
    is_active: bool = True
    is_listed_edinet: bool = False  # EDINETコードリストに基づく上場判定
    last_submitted_at: Optional[str] = None
    former_edinet_codes: Optional[str] = None  # 集約ブリッジ: 旧コード (カンマ区切り)


class ListingEvent(BaseModel):
    """上場・廃止イベントの記録モデル"""

    code: str
    type: str  # LISTING, DELISTING
    event_date: str
    note: Optional[str] = None


class IndexEvent(BaseModel):
    """指数採用・除外イベントの記録モデル"""

    index_name: str
    code: str
    type: str  # ADITION, REMOVAL
    event_date: str


class NameChangeEvent(BaseModel):
    """社名変更イベントの記録モデル"""

    code: str
    old_name: str
    new_name: str
    change_date: str
