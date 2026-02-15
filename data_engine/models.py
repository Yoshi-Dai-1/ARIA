from typing import Optional

from pydantic import BaseModel


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


class CatalogRecord(BaseModel):
    """統合ドキュメントカタログ (documents_index.parquet) のレコードモデル (26カラム構成)"""

    # 1. Identifiers (識別子・基本情報)
    doc_id: str
    jcn: Optional[str] = None  # 法人番号 (Japan Corporate Number)
    code: str  # 証券コード (5桁)
    company_name: str
    edinet_code: Optional[str] = None
    issuer_edinet_code: Optional[str] = None  # 発行者EDINETコード
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
    disclosure_status: Optional[str] = None  # 開示ステータス (1:OK, 2:修正 etc.)
    current_report_reason: Optional[str] = None  # 臨時報告書の提出理由

    # 5. Infrastructure (システム管理情報)
    raw_zip_path: Optional[str] = None
    pdf_path: Optional[str] = None
    processed_status: Optional[str] = "success"
    source: str = "EDINET"


class StockMasterRecord(BaseModel):
    """銘柄マスタ (stocks_master.parquet) のレコードモデル"""

    code: str
    company_name: str
    sector: Optional[str] = None
    market: Optional[str] = None
    is_active: Optional[bool] = None
    last_submitted_at: Optional[str] = None  # 時系列ガード用：情報の最新性を担保する提出日時


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
