from typing import Dict, Iterable

from column_mapper import map_columns_to_canonical
from column_normalizer import normalize_columns
from file_family_detector import detect_file_family
from readiness_checker import compute_readiness
from import_config import MANDATORY_CANONICAL_FIELDS


def inspect_columns(column_names: Iterable[str]) -> Dict:
    cols = list(column_names)
    normalized_columns = normalize_columns(cols)
    file_family = detect_file_family(normalized_columns)
    mapped_fields = map_columns_to_canonical(cols)
    readiness = compute_readiness(file_family, mapped_fields)

    required_fields = MANDATORY_CANONICAL_FIELDS.get(file_family, [])
    required_count = len(required_fields)
    required_mapped = sum(1 for field in required_fields if field in mapped_fields)

    total_columns = len(cols)
    mapped_count = len(mapped_fields)

    coverage_score = (mapped_count / total_columns) if total_columns > 0 else 0.0
    required_score = (required_mapped / required_count) if required_count > 0 else 1.0

    final_score = round((coverage_score * 0.6) + (required_score * 0.4), 2)
    final_score = min(final_score, 1.0)

    return {
        "file_family": file_family,
        "normalized_columns": normalized_columns,
        "mapped_fields": mapped_fields,
        "readiness": readiness,
        "confidence_score": final_score,
    }
