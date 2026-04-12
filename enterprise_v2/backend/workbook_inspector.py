from workbook_reader import list_workbook_sheets
from header_detector import detect_header_row
from excel_reader import read_excel_columns, read_excel_preview
from import_inspector import inspect_columns


def inspect_workbook(file_path: str) -> dict:
    sheet_names = list_workbook_sheets(file_path)
    sheets = []

    for sheet_name in sheet_names:
        header_row = detect_header_row(file_path, sheet_name=sheet_name)
        columns = read_excel_columns(file_path, sheet_name=sheet_name, header_row=header_row)
        result = inspect_columns(columns)
        preview = read_excel_preview(file_path, sheet_name=sheet_name, header_row=header_row, rows=3)

        sheets.append({
            "sheet_name": sheet_name,
            "header_row": header_row,
            "columns": columns,
            "preview_rows": preview,
            "inspection": result,
        })

    best_sheet = max(sheets, key=lambda s: s["inspection"]["confidence_score"], default=None)

    return {
        "sheet_names": sheet_names,
        "selected_sheet": best_sheet["sheet_name"] if best_sheet else None,
        "selected_header_row": best_sheet["header_row"] if best_sheet else 0,
        "sheets": sheets,
        "summary": best_sheet["inspection"] if best_sheet else {
            "file_family": "unknown",
            "normalized_columns": [],
            "mapped_fields": {},
            "readiness": {"available_fields": [], "missing_required_fields": []},
            "confidence_score": 0.0,
        },
    }
