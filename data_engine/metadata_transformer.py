import re
from datetime import datetime


class MetadataTransformer:
    """
    EDINET APIのメタデータとプロセス状態から、
    Data Lakehouse (Catalog) 用のレコードを生成する共通ロジック。

    main.py (ETL) と integrity_audit.py (監査) の両方で使用し、
    ロジックの完全一致を保証する。
    """

    @staticmethod
    def normalize_code(code_str: str) -> str:
        """証券コードの5桁正規化 (末尾0付与)"""
        if not code_str:
            return "99990"
        code_str = str(code_str).strip()
        # 4桁の場合は末尾に0を付加
        if re.match(r"^\d{4}$", code_str):
            return f"{code_str}0"
        return code_str

    @staticmethod
    def transform(
        row: dict, docid: str, title: str, zip_ok: bool, pdf_ok: bool, raw_base_dir, force_status=None
    ) -> dict:
        """
        メタデータ(row)からカタログレコード(dict)への変換を行う純粋関数。

        Args:
            row (dict): EDINET APIからの生のメタデータ辞書
            docid (str): ドキュメントID
            title (str): ドキュメントタイトル
            zip_ok (bool): XBRL(ZIP)が存在するか
            pdf_ok (bool): PDFが存在するか
            raw_base_dir (Path): RAWデータの基準ディレクトリ (相対パス計算用)
            force_status (str, optional): 強制的に設定するステータス (pending/retracted等)

        Returns:
            dict: カタログに保存すべきレコード
        """
        dtc = row.get("docTypeCode")
        ord_c = row.get("ordinanceCode")
        form_c = row.get("formCode")

        # 期末日・決算年度・決算期間（月数）の抽出 (NULL正規化)
        period_start = (row.get("periodStart") or "").strip() or None
        period_end = (row.get("periodEnd") or "").strip() or None

        fiscal_year = int(period_end[:4]) if period_end else None

        # 決算期の月数を算出 (変則決算対応)
        num_months = None
        if period_start and period_end:
            try:
                d1 = datetime.strptime(period_start, "%Y-%m-%d")
                d2 = datetime.strptime(period_end, "%Y-%m-%d")
                diff_days = (d2 - d1).days + 1
                calc_months = round(diff_days / 30.4375)
                if 1 <= calc_months <= 24:
                    num_months = calc_months
                else:
                    num_months = None
            except Exception:
                num_months = None

        sec_code = MetadataTransformer.normalize_code(row.get("secCode", ""))

        # 訂正フラグの厳密判定
        parent_id = row.get("parentDocID")
        is_amendment = parent_id is not None or str(dtc).endswith("1") or "訂正" in (title or "")

        # ステータス判定
        w_status = row.get("withdrawalStatus")
        final_status = "pending"
        if w_status == "1":
            final_status = "retracted"
        if force_status:
            final_status = force_status

        # ファイルパスの相対化 (RAW_BASE_DIR が提供された場合のみ計算、監査時はNoneの場合あり)
        # 注意: main.py では raw_zip/raw_pdf は絶対パス。
        # ここでは「計算済み」の raw_zip/raw_pdf パスを受け取るのではなく、
        # 保存ロジックと分離するため、パス生成自体は呼び出し元で行う設計とするか、
        # あるいは「存在フラグ」だけ受け取ってパス構造の正解をここで定義するか。
        # -> 「パス構造の正解」もここで定義すべき。

        # パス生成ロジックも共通化
        submit_date_str = row.get("submitDateTime")
        rel_zip_path = None
        rel_pdf_path = None

        if submit_date_str:
            try:
                sd = datetime.strptime(submit_date_str, "%Y-%m-%d %H:%M")
                # edinet/year=YYYY/month=MM/day=DD/{docid}.zip
                rel_dir = f"edinet/year={sd.year}/month={sd.month:02d}/day={sd.day:02d}"
                if zip_ok:
                    rel_zip_path = f"{rel_dir}/{docid}.zip"
                if pdf_ok:
                    rel_pdf_path = f"{rel_dir}/{docid}.pdf"
            except:
                pass

        # レコード生成
        record = {
            "doc_id": docid,
            "jcn": (row.get("JCN") or "").strip() or None,
            "code": sec_code,
            "company_name": (row.get("filerName") or "").strip() or "Unknown",
            "edinet_code": (row.get("edinetCode") or "").strip() or None,
            "issuer_edinet_code": (row.get("issuerEdinetCode") or "").strip() or None,
            "fund_code": (row.get("fundCode") or "").strip() or None,
            "submit_at": (row.get("submitDateTime") or "").strip() or None,
            "fiscal_year": fiscal_year,
            "period_start": period_start,
            "period_end": period_end,
            "num_months": num_months,
            "accounting_standard": None,
            "doc_type": dtc or "",
            "title": (title or "").strip() or None,
            "form_code": (form_c or "").strip() or None,
            "ordinance_code": (ord_c or "").strip() or None,
            "is_amendment": is_amendment,
            "parent_doc_id": (parent_id or "").strip() or None,
            "withdrawal_status": (w_status or "").strip() or None,
            "disclosure_status": (row.get("disclosureStatus") or "").strip() or None,
            "current_report_reason": (row.get("currentReportReason") or "").strip() or None,
            "raw_zip_path": rel_zip_path,
            "pdf_path": rel_pdf_path,
            "processed_status": final_status,
            "source": "EDINET",
        }
        return record
