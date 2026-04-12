from typing import Dict, Iterable

from column_normalizer import normalize_column_name
from column_synonyms import COLUMN_SYNONYMS


def map_columns_to_canonical(column_names: Iterable[str]) -> Dict[str, str]:
    normalized_source = {
        original: normalize_column_name(original)
        for original in column_names
    }

    result: Dict[str, str] = {}

    for canonical_field, synonyms in COLUMN_SYNONYMS.items():
        normalized_synonyms = {normalize_column_name(s) for s in synonyms}

        for original_name, normalized_name in normalized_source.items():
            if normalized_name in normalized_synonyms:
                result[canonical_field] = original_name
                break

    return result
