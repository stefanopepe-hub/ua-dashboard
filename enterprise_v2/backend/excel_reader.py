import pandas as pd


def read_excel_columns(file_path: str, sheet_name: str | None = None) -> list[str]:
    dataframe = pd.read_excel(file_path, sheet_name=sheet_name, nrows=5)
    return [str(column) for column in dataframe.columns]
