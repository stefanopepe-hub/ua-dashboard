from typing import Optional
import pandas as pd


def detect_header_row(file_path: str, sheet_name: str | None = None, max_rows: int = 8) -> int:
    preview = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=max_rows)
    best_row = 0
    best_score = -1

    for idx in range(len(preview)):
        row = preview.iloc[idx].tolist()
        values = [str(v).strip() for v in row if str(v).strip() and str(v).strip().lower() != "nan"]
        unique_count = len(set(values))
        score = unique_count + len(values)
        if score > best_score:
            best_row = idx
            best_score = score

    return int(best_row)
