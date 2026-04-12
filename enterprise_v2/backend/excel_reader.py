import pandas as pd


def read_excel_columns(file_path: str, sheet_name: str | None = None, header_row: int = 0) -> list[str]:
    dataframe = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row, nrows=5)
    return [str(column) for column in dataframe.columns]


def read_excel_preview(file_path: str, sheet_name: str | None = None, header_row: int = 0, rows: int = 5) -> list[dict]:
    dataframe = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row, nrows=rows)
    dataframe = dataframe.fillna("")
    return dataframe.astype(str).to_dict(orient="records")
