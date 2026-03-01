import math
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
    is_consolidated: Optional[bool] = None  # 連結の有無 (True/False)
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
        """物理的事実に基づき、NaN/空欄/- を None に、有/無を bool に正規化する"""
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        if isinstance(v, str):
            s_v = v.strip()
            if s_v.lower() in ["nan", "none", "", "-"]:
                return None
            if s_v == "有":
                return True
            if s_v == "無":
                return False
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
    """
    ARIA 銘柄マスタレコード (Perfect Integrity)
    識別子、属性、業界、状態の論理的順序で構成。
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    # --- 1. 識別子 (Identifiers) ---
    edinet_code: Optional[str] = Field(None, description="EDINETコード")
    code: Optional[str] = Field(None, description="証券コード (5桁正規化)")
    parent_code: Optional[str] = Field(None, description="親銘柄コード (優先株などの場合)")
    jcn: Optional[str] = Field(None, description="法人番号 (13桁)")

    # --- 2. 基本属性 (Basic Attributes / EDINET系) ---
    company_name: str = Field(..., description="提出者名 (和文)")
    company_name_en: Optional[str] = Field(None, description="提出者名 (英文)")
    submitter_name_kana: Optional[str] = Field(None, description="提出者名 (ヨミ)")
    submitter_type: Optional[str] = Field(None, description="提出者種別")
    is_consolidated: Optional[bool] = Field(None, description="連結の有無 (True/False)")
    capital: Optional[float] = Field(None, description="資本金 (単位: 百万円)")
    settlement_date: Optional[str] = Field(None, description="決算日")
    address: Optional[str] = Field(None, description="所在地")

    # --- 3. 業界・市場属性 (Industry & Market / JPX系) ---
    sector_jpx_33: Optional[str] = Field(None, description="JPX 33業種区分")
    sector_jpx_17: Optional[str] = Field(None, description="JPX 17業種区分")
    industry_edinet: Optional[str] = Field(None, description="EDINET業種区分 (和文)")
    industry_edinet_en: Optional[str] = Field(None, description="EDINET業種区分 (英文)")
    market: Optional[str] = Field(None, description="上場市場名")

    # --- 4. 状態とライフサイクル (Status & Lifecycle) ---
    is_active: bool = Field(True, description="ARIA 収集・追跡対象フラグ (運用の真実)")
    is_listed_edinet: bool = Field(True, description="EDINET公式名簿 上場フラグ (法令の真実)")
    last_submitted_at: Optional[str] = Field(None, description="最終書類提出日時")
    former_edinet_codes: Optional[str] = Field(None, description="旧EDINETコード (集約ブリッジ用)")

    @field_validator(
        "edinet_code",
        "jcn",
        "company_name_en",
        "submitter_name_kana",
        "submitter_type",
        "settlement_date",
        "address",
        "sector_jpx_33",
        "sector_jpx_17",
        "industry_edinet",
        "industry_edinet_en",
        "market",
        "last_submitted_at",
        "former_edinet_codes",
        "is_consolidated",  # is_consolidated は nan_to_none で処理
        mode="before",
    )
    @classmethod
    def nan_to_none(cls, v: Any) -> Any:
        """pandas の NaN や文字列 'nan' を物理的に排除し、工学的主権を保つ"""
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        if isinstance(v, str) and (v.lower() in ["nan", "none", "", "-"]):
            return None

        # 連結フラグの Bool 変換 (有/無 -> True/False)
        if isinstance(v, str):
            s_v = v.strip()
            if s_v == "有":
                return True
            if s_v == "無":
                return False
        if v in [1, True]:
            return True
        if v in [0, False]:
            return False

        return str(v).strip()

    # 証券コードを5桁に正規化 (SICC準拠)
    @field_validator("code", "parent_code", mode="before")
    @classmethod
    def normalize_sec_code(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        s_v = str(v).strip()
        if not s_v or s_v == "nan":
            return None
        if len(s_v) == 4:
            return s_v + "0"
        return s_v

    @field_validator("is_listed_edinet", mode="before")
    @classmethod
    def convert_to_bool_listed(cls, v: Any) -> bool:
        """「上場/非上場」の文字列を物理的な bool へ変換する"""
        if isinstance(v, bool):
            return v
        s_v = str(v).strip()
        if s_v == "上場":
            return True
        if s_v == "非上場":
            return False
        return True  # デフォルトは上場扱い


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
