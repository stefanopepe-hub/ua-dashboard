from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import tempfile
import os

from import_inspector import inspect_columns
from workbook_inspector import inspect_workbook

app = FastAPI(title="Telethon Enterprise V2 API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
        inspection = inspect_workbook(temp_path)
        return {
            "ok": True,
            "file_name": file.filename,
            "file_family": inspection["summary"]["file_family"],
            "normalized_columns": inspection["summary"]["normalized_columns"],
            "mapped_fields": inspection["summary"]["mapped_fields"],
            "readiness": inspection["summary"]["readiness"],
            "confidence_score": inspection["summary"]["confidence_score"],
            "sheet_names": inspection["sheet_names"],
            "selected_sheet": inspection["selected_sheet"],
            "selected_header_row": inspection["selected_header_row"],
            "sheets": inspection["sheets"],
        }
    except Exception as exc:
        return {
            "ok": False,
            "file_name": file.filename,
            "file_family": "unknown",
            "normalized_columns": [],
            "mapped_fields": {},
            "readiness": {"available_fields": [], "missing_required_fields": []},
            "confidence_score": 0.0,
            "sheet_names": [],
            "selected_sheet": None,
            "selected_header_row": 0,
            "sheets": [],
            "error_message": f"Impossibile ispezionare il file: {exc}",
        }
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
