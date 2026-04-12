from pathlib import Path
import json

BASE = Path(__file__).parent / "data"

def _load(name: str) -> dict:
    return json.loads((BASE / name).read_text(encoding="utf-8"))

def load_sample_analytics() -> dict:
    return _load("sample_saving_analytics.json")

def load_sample_resources() -> dict:
    return _load("sample_resources_analytics.json")

def load_sample_cycle() -> dict:
    return _load("sample_cycle_analytics.json")

def load_sample_nc() -> dict:
    return _load("sample_nc_analytics.json")
