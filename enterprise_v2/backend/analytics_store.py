from __future__ import annotations
from pathlib import Path
import json

DATA_FILE = Path(__file__).parent / "data" / "sample_saving_analytics.json"


def load_sample_analytics() -> dict:
    return json.loads(DATA_FILE.read_text(encoding="utf-8"))
