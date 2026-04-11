from typing import Dict, Iterable

from column_mapper import map_columns_to_canonical
from column_normalizer import normalize_columns
from file_family_detector import detect_file_family
from readiness_checker import compute_readiness


def inspect_columns(column_names: Iterable[str]) -> Dict:
    normalized_columns = normalize_columns(column_names)
    file_family = detect_file_family(normalized_columns)
    mapped_fields = map_columns_to_canonical(column_names)
    readiness = compute_readiness(file_family, mapped_fields)

    return {
        "file_family": file_family,
        "normalized_columns": normalized_columns,
        "mapped_fields": mapped_fields,
        "readiness": readiness,
    }
