from fastapi import FastAPI, File, UploadFile
from pydantic import BaseModel
import tempfile
import os

from import_inspector import inspect_columns
from excel_reader import read_excel_columns
from workbook_reader import list_workbook_sheets

app = FastAPI(title="Telethon Enterprise V2 API")


class InspectColumnsRequest(BaseModel):
    columns: list[str]


@app.get("/health")
def health():
    return {"ok": True, "service": "enterprise_v2_backend"}


@app.post("/inspect-columns")
def inspect_columns_endpoint(payload: InspectColumnsRequest):
    return inspect_columns(payload.columns)


@app.post("/inspect-excel")
async def inspect_excel(file: UploadFile = File(...)):
    suffix = os.path.splitext(file.filename)[1] or ".xlsx"

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
        content = await file.read()
        temp_file.write(content)
        temp_path = temp_file.name

    try:
        sheets = list_workbook_sheets(temp_path)
        columns = read_excel_columns(temp_path)
        result = inspect_columns(columns)
        result["file_name"] = file.filename
        result["sheet_names"] = sheets
        return result
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
