from fastapi import FastAPI
from pydantic import BaseModel

from import_inspector import inspect_columns

app = FastAPI(title="Telethon Enterprise V2 API")


class InspectColumnsRequest(BaseModel):
    columns: list[str]


@app.get("/health")
def health():
    return {"ok": True, "service": "enterprise_v2_backend"}


@app.post("/inspect-columns")
def inspect_columns_endpoint(payload: InspectColumnsRequest):
    return inspect_columns(payload.columns)
