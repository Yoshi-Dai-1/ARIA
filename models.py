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
    """統合ドキュメントカタログ (documents_index.parquet) のレコードモデル (18カラム構成 - 解析最適化順序)"""

    # 1. Identifiers (識別子・基本情報)
    doc_id: str
    code: str
    company_name: str
    edinet_code: Optional[str] = None

    # 2. Timeline (時間軸 - 解析における最重要キー)
    submit_at: str

    # 3. Domain/Fiscal (決算・期間属性)
    fiscal_year: Optional[int] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    num_months: Optional[int] = None
    is_amendment: bool = False

    # 4. Document Details (書類詳細特性)
    doc_type: str
    title: str
    form_code: Optional[str] = None
    ordinance_code: Optional[str] = None

    # 5. Infrastructure (システム管理情報)
    raw_zip_path: Optional[str] = None
    pdf_path: Optional[str] = None
    processed_status: Optional[str] = "success"
    source: str = "EDINET"


class StockMasterRecord(BaseModel):
    """銘柄マスタ (stocks_master.parquet) のレコードモデル"""

    code: str
    company_name: str
    sector: Optional[str] = "その他"
    market: Optional[str] = None
    is_active: bool = True


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
