from excel_reader import read_excel_columns
from import_inspector import inspect_columns


def select_best_sheet(file_path: str, sheet_names: list[str]) -> tuple[str | None, dict]:
    best_sheet = None
    best_result = {
        "confidence_score": -1.0,
        "file_family": "unknown",
        "normalized_columns": [],
        "mapped_fields": {},
        "readiness": {"available_fields": [], "missing_required_fields": []},
    }

    for sheet_name in sheet_names:
        columns = read_excel_columns(file_path, sheet_name=sheet_name)
        result = inspect_columns(columns)
        if result["confidence_score"] > best_result["confidence_score"]:
            best_sheet = sheet_name
            best_result = result

    return best_sheet, best_result
