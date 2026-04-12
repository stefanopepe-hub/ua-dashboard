from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import tempfile
import os

from import_inspector import inspect_columns
from workbook_inspector import inspect_workbook
from analytics_store import (
    load_sample_analytics,
    load_sample_resources,
    load_sample_cycle,
    load_sample_nc,
)

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

@app.get("/analytics/saving/summary")
def saving_summary():
    return {"ok": True, "data": load_sample_analytics()["summary"]}

@app.get("/analytics/saving/top-suppliers")
def saving_top_suppliers():
    return {"ok": True, "data": load_sample_analytics()["top_suppliers"]}

@app.get("/analytics/saving/document-types")
def saving_document_types():
    return {"ok": True, "data": load_sample_analytics()["document_types"]}

@app.get("/analytics/saving/cdc")
def saving_cdc():
    return {"ok": True, "data": load_sample_analytics()["cdc_breakdown"]}

@app.get("/analytics/saving/buyers")
def saving_buyers():
    return {"ok": True, "data": load_sample_analytics()["buyers"]}

@app.get("/analytics/saving/protocols")
def saving_protocols():
    return {"ok": True, "data": load_sample_analytics()["protocols"]}

@app.get("/analytics/saving/yoy")
def saving_yoy():
    return {"ok": True, "data": load_sample_analytics()["yoy"]}

@app.get("/analytics/resources/summary")
def resources_summary():
    return {"ok": True, "data": load_sample_resources()["summary"]}

@app.get("/analytics/resources/list")
def resources_list():
    return {"ok": True, "data": load_sample_resources()["resources"]}

@app.get("/analytics/resources/monthly-trend")
def resources_monthly_trend():
    return {"ok": True, "data": load_sample_resources()["monthly_trend"]}

@app.get("/analytics/cycle/summary")
def cycle_summary():
    return {"ok": True, "data": load_sample_cycle()["summary"]}

@app.get("/analytics/cycle/bottlenecks")
def cycle_bottlenecks():
    return {"ok": True, "data": load_sample_cycle()["bottlenecks"]}

@app.get("/analytics/cycle/monthly-trend")
def cycle_monthly_trend():
    return {"ok": True, "data": load_sample_cycle()["monthly_trend"]}

@app.get("/analytics/nc/summary")
def nc_summary():
    return {"ok": True, "data": load_sample_nc()["summary"]}

@app.get("/analytics/nc/top-suppliers")
def nc_top_suppliers():
    return {"ok": True, "data": load_sample_nc()["top_suppliers"]}

@app.get("/analytics/nc/types")
def nc_types():
    return {"ok": True, "data": load_sample_nc()["types"]}

@app.get("/analytics/nc/monthly-trend")
def nc_monthly_trend():
    return {"ok": True, "data": load_sample_nc()["monthly_trend"]}
