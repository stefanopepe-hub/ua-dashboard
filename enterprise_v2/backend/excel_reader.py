import pandas as pd


def read_excel_columns(file_path: str) -> list[str]:
    dataframe = pd.read_excel(file_path, nrows=5)
    return [str(column) for column in dataframe.columns]
