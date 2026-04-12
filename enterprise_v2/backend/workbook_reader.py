import pandas as pd


def list_workbook_sheets(file_path: str) -> list[str]:
    workbook = pd.ExcelFile(file_path)
    return list(workbook.sheet_names)
