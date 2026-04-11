from fastapi import FastAPI

app = FastAPI(title="Telethon Enterprise V2 API")


@app.get("/health")
def health():
    return {"ok": True, "service": "enterprise_v2_backend"}
