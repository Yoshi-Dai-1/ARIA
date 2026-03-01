import json
from pathlib import Path

CONFIG_PATH = Path(__file__).parent / "aria_config.json"


def _load_config():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing absolute configuration file: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# Load config strictly at module import time
__config = _load_config()

# Single Source of Truth validation for ARIA_SCOPE
__raw_scope = __config.get("aria_scope")

# Fail-Fast mechanism: If not set or empty, crash immediately.
if not __raw_scope or not str(__raw_scope).strip():
    raise ValueError(
        "CRITICAL ERROR: 'aria_scope' is missing or empty in aria_config.json. "
        "It must be explicitly defined as 'Listed', 'Unlisted', or 'All'."
    )

# Case-insensitive normalization
__normalized_scope = str(__raw_scope).strip().capitalize()

if __normalized_scope not in ["Listed", "Unlisted", "All"]:
    raise ValueError(
        f"CRITICAL ERROR: Invalid 'aria_scope' value '{__raw_scope}'. It must be one of 'Listed', 'Unlisted', or 'All'."
    )

ARIA_SCOPE = __normalized_scope
TSE_URL = __config.get("tse_url")
HF_WARNING_THRESHOLD = __config.get("hf_folder_file_warning_threshold", 7000)
