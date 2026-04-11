from typing import Dict, List

from import_config import MANDATORY_CANONICAL_FIELDS


def compute_readiness(file_family: str, mapped_fields: Dict[str, str]) -> Dict[str, List[str]]:
    required_fields = MANDATORY_CANONICAL_FIELDS.get(file_family, [])
    available = list(mapped_fields.keys())
    missing = [field for field in required_fields if field not in mapped_fields]

    return {
        "available_fields": available,
        "missing_required_fields": missing,
    }
