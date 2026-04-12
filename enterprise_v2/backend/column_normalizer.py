import re
from typing import Iterable, List


def normalize_column_name(name: str) -> str:
    value = str(name or "").strip().lower()
    value = value.replace("€", " eur ")
    value = re.sub(r"[._/\\\\-]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_columns(column_names: Iterable[str]) -> List[str]:
    return [normalize_column_name(c) for c in column_names]
