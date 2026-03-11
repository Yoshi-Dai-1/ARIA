from enum import Enum
from typing import Any, Dict, Optional, Tuple

from data_engine.core.utils import normalize_code


class ProcessVerdict(str, Enum):
    """ARIA における書類処理の最終判定（司法判断）"""

    PARSE = "parse"  # フル解析対象（有報等）
    SAVE_METADATA = "save_meta"  # メタデータ保存のみ（非解析対象）
    SKIP_PROCESSED = "skip_already_processed"  # 既処理によるスキップ
    SKIP_OUT_OF_SCOPE = "skip_out_of_scope"  # スコープ外（Listed/Unlisted設定）によるスキップ
    SKIP_WITHDRAWN = "skip_withdrawn"  # 取下げ済みによるスキップ


class SkipReason(str, Enum):
    """フィルタリングでスキップされた詳細理由"""

    NONE = "none"
    WITHDRAWN = "withdrawn"
    ALREADY_PROCESSED = "already_processed"
    NO_SEC_CODE = "no_sec_code"  # 上場検索で証券コードなし
    INVALID_CODE_LENGTH = "invalid_code_length"  # 証券コードの形式不正
    HAS_SEC_CODE = "has_sec_code"  # 非上場検索で証券コードあり
    REACTION_REQUIRED = "retraction_sync_required"  # 取下げ同期が必要（スキップしない）


class FilteringEngine:
    """
    ARIA の「憲法の番人」
    あらゆる書類に対し、ARIA_SCOPE と物理的事実に基づき一貫した処理判定を下す。
    将来の米国株（US:Ticker）や TDNet への拡張を考慮した設計。
    """

    def __init__(self, aria_scope: str = "All"):
        self.aria_scope = aria_scope
        # 解析対象の定義 (将来的に別定義ファイルやDBへ移行可能)
        self.PARSE_FORM_CODES = ["030000"]  # 有価証券報告書
        self.PARSE_DOC_TYPE_CODES = ["120"]
        self.PARSE_ORDINANCE_CODES = ["010"]  # 企業内容等の開示に関する内閣府令

    def get_verdict(
        self,
        row: Dict[str, Any],
        is_processed: bool = False,
        local_status: Optional[str] = None,
    ) -> Tuple[ProcessVerdict, SkipReason]:
        """
        書類メタデータを統合的に判定する。

        Args:
            row: EDINET/TDNet 等のメタデータ行
            is_processed: 既にカタログに存在するかどうか
            local_status: ローカル（カタログ）側でのステータス

        Returns:
            (ProcessVerdict, SkipReason)
        """
        # 1. 取下げチェック (物理的事実の優先)
        is_withdrawn = str(row.get("withdrawalStatus")) == "1"

        # 2. 既処理チェックと取下げ同期の特殊判定
        if is_processed:
            # 取下げステータスの同期が必要な場合（APIで取下げだがローカルが未反映）はスキップしない
            if is_withdrawn and local_status != "retracted":
                pass  # そのまま後続の判定へ（ステータス更新のため）
            else:
                return ProcessVerdict.SKIP_PROCESSED, SkipReason.ALREADY_PROCESSED

        if is_withdrawn:
            return ProcessVerdict.SKIP_WITHDRAWN, SkipReason.WITHDRAWN

        if self.aria_scope == "Listed":
            norm_code = normalize_code(row.get("secCode"), nationality="JP")
            if not norm_code:
                return ProcessVerdict.SKIP_OUT_OF_SCOPE, SkipReason.NO_SEC_CODE
            
            # プレフィックスを剥離して長さをチェック
            core_code = norm_code.split(":", 1)[1] if ":" in norm_code else norm_code
            if len(core_code) < 4:
                return ProcessVerdict.SKIP_OUT_OF_SCOPE, SkipReason.INVALID_CODE_LENGTH
        elif self.aria_scope == "Unlisted":
            norm_code = normalize_code(row.get("secCode"), nationality="JP")
            if norm_code:
                core_code = norm_code.split(":", 1)[1] if ":" in norm_code else norm_code
                if len(core_code) >= 4:
                    return ProcessVerdict.SKIP_OUT_OF_SCOPE, SkipReason.HAS_SEC_CODE

        # 4. 解析対象判定 (工場への仕振り)
        # TODO: 将来的に edinet/tdnet/sec 別の判定クラスへ委譲する
        form_code = row.get("formCode") or row.get("form")
        doc_type = row.get("docTypeCode") or row.get("type")
        ordinance = row.get("ordinanceCode") or row.get("ord")

        is_yuho = (
            doc_type in self.PARSE_DOC_TYPE_CODES
            and form_code in self.PARSE_FORM_CODES
            and ordinance in self.PARSE_ORDINANCE_CODES
        )

        if is_yuho:
            return ProcessVerdict.PARSE, SkipReason.NONE

        return ProcessVerdict.SAVE_METADATA, SkipReason.NONE
