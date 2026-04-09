import os
import io
from datetime import date
from typing import Optional
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="UA Dashboard API", version="1.0.0")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:5173").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ─────────────────────────────────────────────
# UPLOAD ENDPOINTS
# ─────────────────────────────────────────────

@app.post("/upload/saving")
async def upload_saving(file: UploadFile = File(...)):
    """Carica il file Excel saving (foglio: Final saving 2025)"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), sheet_name="Final saving 2025")
    except Exception:
        raise HTTPException(400, "Foglio 'Final saving 2025' non trovato nel file Excel")

    df.columns = [c.strip() for c in df.columns]
    df = df.dropna(subset=["Data doc."])

    sb = get_supabase()
    log = sb.table("upload_log").insert({"filename": file.filename, "tipo": "saving"}).execute()
    upload_id = log.data[0]["id"]

    records = []
    for _, row in df.iterrows():
        accred = str(row.get("Accred.albo", "NO")).strip().upper() in ("SI", "SÌ")
        neg = str(row.get("Negoziazione", "NO")).strip().upper() in ("SI", "SÌ")
        data = str(row.get("Data doc.", ""))
        try:
            data_doc = pd.to_datetime(row["Data doc."]).date().isoformat()
        except Exception:
            continue

        records.append({
            "upload_id": upload_id,
            "cod_utente": int(row["Cod.utente"]) if pd.notna(row.get("Cod.utente")) else None,
            "utente": str(row.get("Utente", "")) or None,
            "num_doc": int(row["Num.doc."]) if pd.notna(row.get("Num.doc.")) else None,
            "data_doc": data_doc,
            "alfa_documento": str(row.get("Alfa documento", "")) or None,
            "str_ric": str(row.get("Str./Ric.", "")) or None,
            "stato_dms": str(row.get("Stato DMS", "")) or None,
            "codice_fornitore": int(row["Codice fornitore"]) if pd.notna(row.get("Codice fornitore")) else None,
            "ragione_sociale": str(row.get("Ragione sociale fornitore", "")) or None,
            "accred_albo": accred,
            "protoc_ordine": float(row["Protoc.ordine"]) if pd.notna(row.get("Protoc.ordine")) else None,
            "protoc_commessa": str(row.get("Protoc.commessa", "")) or None,
            "grp_merceol": float(row["Grp.merceol."]) if pd.notna(row.get("Grp.merceol.")) else None,
            "desc_gruppo_merceol": str(row.get("Descrizione gruppo merceologic", "")) or None,
            "centro_di_costo": str(row.get("Centro di costo", "")) or None,
            "desc_cdc": str(row.get("Descrizione centro di costo", "")) or None,
            "valuta": str(row.get("Valuta", "EURO")) or "EURO",
            "imp_iniziale": float(row["Imp.iniziale"]) if pd.notna(row.get("Imp.iniziale")) else 0,
            "imp_negoziato": float(row["Imp.negoziato"]) if pd.notna(row.get("Imp.negoziato")) else 0,
            "saving_val": float(row["Saving"]) if pd.notna(row.get("Saving")) else 0,
            "perc_saving": float(row["% Saving"]) if pd.notna(row.get("% Saving")) else 0,
            "negoziazione": neg,
            "imp_iniziale_eur": float(row["Imp. Iniziale €"]) if pd.notna(row.get("Imp. Iniziale €")) else 0,
            "imp_negoziato_eur": float(row["Imp. Negoziato €"]) if pd.notna(row.get("Imp. Negoziato €")) else 0,
            "saving_eur": float(row["Saving.1"]) if pd.notna(row.get("Saving.1")) else 0,
            "perc_saving_eur": float(row["%saving"]) if pd.notna(row.get("%saving")) else 0,
            "cdc": str(row.get("CDC ", "")) or None,
            "cambio": float(row["cambio"]) if pd.notna(row.get("cambio")) else 1,
        })

    # Batch insert in chunks of 500
    for i in range(0, len(records), 500):
        sb.table("saving").insert(records[i:i+500]).execute()

    sb.table("upload_log").update({"rows_inserted": len(records)}).eq("id", upload_id).execute()
    return {"status": "ok", "rows": len(records), "upload_id": upload_id}


@app.post("/upload/tempi")
async def upload_tempi(file: UploadFile = File(...)):
    """Carica il file Excel tempo attraversamento ordini"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents))
    except Exception:
        raise HTTPException(400, "Errore lettura file")

    sb = get_supabase()
    log = sb.table("upload_log").insert({"filename": file.filename, "tipo": "tempi"}).execute()
    upload_id = log.data[0]["id"]

    records = []
    for _, row in df.iterrows():
        records.append({
            "upload_id": upload_id,
            "protocol": str(row.get("Protocol", "")) or None,
            "year_month": str(row.get("Year_Month", "")) or None,
            "days_purchasing": float(row["Days_Purchasing"]) if pd.notna(row.get("Days_Purchasing")) else 0,
            "days_auto": float(row["Days_Auto"]) if pd.notna(row.get("Days_Auto")) else 0,
            "days_other": float(row["Days_Other"]) if pd.notna(row.get("Days_Other")) else 0,
            "total_days": float(row["Total_Days"]) if pd.notna(row.get("Total_Days")) else 0,
            "perc_purchasing": float(row["Perc_Purchasing"]) if pd.notna(row.get("Perc_Purchasing")) else 0,
            "perc_auto": float(row["Perc_Auto"]) if pd.notna(row.get("Perc_Auto")) else 0,
            "perc_other": float(row["Perc_Other"]) if pd.notna(row.get("Perc_Other")) else 0,
            "bottleneck": str(row.get("Bottleneck", "")) or None,
        })

    for i in range(0, len(records), 500):
        sb.table("tempo_attraversamento").insert(records[i:i+500]).execute()

    sb.table("upload_log").update({"rows_inserted": len(records)}).eq("id", upload_id).execute()
    return {"status": "ok", "rows": len(records), "upload_id": upload_id}


@app.post("/upload/nc")
async def upload_nc(file: UploadFile = File(...)):
    """Carica il file Excel non conformità"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents))
    except Exception:
        raise HTTPException(400, "Errore lettura file")

    sb = get_supabase()
    log = sb.table("upload_log").insert({"filename": file.filename, "tipo": "nc"}).execute()
    upload_id = log.data[0]["id"]

    records = []
    for _, row in df.iterrows():
        def safe_date(col):
            try:
                return pd.to_datetime(row[col]).date().isoformat()
            except Exception:
                return None

        nc_val = str(row.get("Non Conformità", "NO")).strip().upper() in ("SI", "SÌ")
        records.append({
            "upload_id": upload_id,
            "protocollo_commessa": str(row.get("Protocollo Commessa", "")) or None,
            "ragione_sociale": str(row.get("Ragione sociale anagrafica", "")) or None,
            "tipo_origine": str(row.get("Tipo Origine", "")) or None,
            "data_origine": safe_date("Data Origine"),
            "utente_origine": str(row.get("Utente Origine", "")) or None,
            "codice_prima_fattura": str(row.get("Codice Prima Fattura", "")) or None,
            "data_prima_fattura": safe_date("Data Prima Fattura"),
            "importo_prima_fattura": float(row["Importo Prima Fattura"]) if pd.notna(row.get("Importo Prima Fattura")) else None,
            "delta_giorni": float(row["Delta giorni (fattura - origine)"]) if pd.notna(row.get("Delta giorni (fattura - origine)")) else None,
            "non_conformita": nc_val,
        })

    for i in range(0, len(records), 500):
        sb.table("non_conformita").insert(records[i:i+500]).execute()

    sb.table("upload_log").update({"rows_inserted": len(records)}).eq("id", upload_id).execute()
    return {"status": "ok", "rows": len(records), "upload_id": upload_id}


# ─────────────────────────────────────────────
# KPI ENDPOINTS — SAVING
# ─────────────────────────────────────────────

@app.get("/kpi/saving/riepilogo")
def kpi_saving_riepilogo(
    anno: Optional[int] = Query(None),
    str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None),
):
    """KPI headline: impegnato totale, saving, % saving, ordini, % negoziati, % albo"""
    sb = get_supabase()
    q = sb.table("saving").select(
        "imp_iniziale_eur,saving_eur,negoziazione,accred_albo,alfa_documento,data_doc,str_ric,cdc"
    )
    if anno:
        q = q.gte("data_doc", f"{anno}-01-01").lte("data_doc", f"{anno}-12-31")
    if str_ric:
        q = q.eq("str_ric", str_ric)
    if cdc:
        q = q.eq("cdc", cdc)

    rows = q.execute().data
    df = pd.DataFrame(rows)

    if df.empty:
        return {"impegnato": 0, "saving": 0, "perc_saving": 0, "n_ordini": 0, "n_negoziati": 0, "perc_negoziati": 0, "perc_albo": 0}

    doc_neg = ["OS", "OSP", "PS", "OPR", "ORN"]
    df_neg = df[df["alfa_documento"].isin(doc_neg)]

    impegnato = df["imp_iniziale_eur"].sum()
    saving = df["saving_eur"].sum()
    n_ordini = len(df_neg)
    n_negoziati = df_neg["negoziazione"].sum()
    n_albo = df["accred_albo"].sum()

    return {
        "impegnato": round(impegnato, 2),
        "saving": round(saving, 2),
        "perc_saving": round(saving / impegnato * 100, 2) if impegnato else 0,
        "n_ordini": int(n_ordini),
        "n_negoziati": int(n_negoziati),
        "perc_negoziati": round(n_negoziati / n_ordini * 100, 2) if n_ordini else 0,
        "n_fornitori_albo": int(n_albo),
        "perc_albo": round(n_albo / len(df) * 100, 2) if len(df) else 0,
    }


@app.get("/kpi/saving/mensile")
def kpi_saving_mensile(
    anno: Optional[int] = Query(None),
    str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None),
):
    """Andamento mensile: impegnato, saving, % saving, ordini, negoziati"""
    sb = get_supabase()
    q = sb.table("saving").select(
        "data_doc,imp_iniziale_eur,saving_eur,negoziazione,alfa_documento,str_ric,cdc"
    )
    if anno:
        q = q.gte("data_doc", f"{anno}-01-01").lte("data_doc", f"{anno}-12-31")
    if str_ric:
        q = q.eq("str_ric", str_ric)
    if cdc:
        q = q.eq("cdc", cdc)

    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return []

    df["mese"] = pd.to_datetime(df["data_doc"]).dt.strftime("%Y-%m")
    doc_neg = ["OS", "OSP", "PS", "OPR", "ORN"]
    df_neg = df[df["alfa_documento"].isin(doc_neg)]

    result = []
    for mese, grp in df.groupby("mese"):
        grp_neg = df_neg[df_neg["mese"] == mese]
        imp = grp["imp_iniziale_eur"].sum()
        sav = grp["saving_eur"].sum()
        n_ord = len(grp_neg)
        n_neg = grp_neg["negoziazione"].sum()
        result.append({
            "mese": mese,
            "impegnato": round(imp, 2),
            "saving": round(sav, 2),
            "perc_saving": round(sav / imp * 100, 2) if imp else 0,
            "n_ordini": int(n_ord),
            "n_negoziati": int(n_neg),
            "perc_negoziati": round(n_neg / n_ord * 100, 2) if n_ord else 0,
        })
    return sorted(result, key=lambda x: x["mese"])


@app.get("/kpi/saving/per-cdc")
def kpi_saving_per_cdc(anno: Optional[int] = Query(None)):
    """Breakdown per CDC"""
    sb = get_supabase()
    q = sb.table("saving").select("cdc,imp_iniziale_eur,saving_eur,negoziazione,accred_albo")
    if anno:
        q = q.gte("data_doc", f"{anno}-01-01").lte("data_doc", f"{anno}-12-31")
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return []

    result = []
    for cdc, grp in df.groupby("cdc"):
        imp = grp["imp_iniziale_eur"].sum()
        sav = grp["saving_eur"].sum()
        result.append({
            "cdc": cdc,
            "impegnato": round(imp, 2),
            "saving": round(sav, 2),
            "perc_saving": round(sav / imp * 100, 2) if imp else 0,
            "n_ordini": len(grp),
            "n_negoziati": int(grp["negoziazione"].sum()),
        })
    return sorted(result, key=lambda x: x["impegnato"], reverse=True)


@app.get("/kpi/saving/per-buyer")
def kpi_saving_per_buyer(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None)):
    """Breakdown per buyer"""
    sb = get_supabase()
    q = sb.table("saving").select("utente,imp_iniziale_eur,saving_eur,negoziazione,cdc,str_ric")
    if anno:
        q = q.gte("data_doc", f"{anno}-01-01").lte("data_doc", f"{anno}-12-31")
    if str_ric:
        q = q.eq("str_ric", str_ric)
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return []

    result = []
    for utente, grp in df.groupby("utente"):
        imp = grp["imp_iniziale_eur"].sum()
        sav = grp["saving_eur"].sum()
        result.append({
            "utente": utente,
            "impegnato": round(imp, 2),
            "saving": round(sav, 2),
            "perc_saving": round(sav / imp * 100, 2) if imp else 0,
            "n_ordini": len(grp),
            "n_negoziati": int(grp["negoziazione"].sum()),
        })
    return sorted(result, key=lambda x: x["saving"], reverse=True)


@app.get("/kpi/saving/top-fornitori")
def kpi_top_fornitori(
    anno: Optional[int] = Query(None),
    per: str = Query("saving", description="saving|impegnato"),
    limit: int = Query(10),
    str_ric: Optional[str] = Query(None),
):
    """Top fornitori per saving o per volume"""
    sb = get_supabase()
    q = sb.table("saving").select("ragione_sociale,imp_iniziale_eur,saving_eur,negoziazione,accred_albo")
    if anno:
        q = q.gte("data_doc", f"{anno}-01-01").lte("data_doc", f"{anno}-12-31")
    if str_ric:
        q = q.eq("str_ric", str_ric)
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return []

    grp = df.groupby("ragione_sociale").agg(
        impegnato=("imp_iniziale_eur", "sum"),
        saving=("saving_eur", "sum"),
        n_ordini=("imp_iniziale_eur", "count"),
        albo=("accred_albo", "first"),
    ).reset_index()
    grp["perc_saving"] = (grp["saving"] / grp["impegnato"] * 100).round(2)
    sort_col = "saving" if per == "saving" else "impegnato"
    top = grp.nlargest(limit, sort_col)
    return top.to_dict(orient="records")


@app.get("/kpi/saving/pareto-fornitori")
def kpi_pareto_fornitori(anno: Optional[int] = Query(None)):
    """Curva Pareto: quanti fornitori coprono l'80% della spesa"""
    sb = get_supabase()
    q = sb.table("saving").select("ragione_sociale,imp_iniziale_eur")
    if anno:
        q = q.gte("data_doc", f"{anno}-01-01").lte("data_doc", f"{anno}-12-31")
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return []

    grp = df.groupby("ragione_sociale")["imp_iniziale_eur"].sum().sort_values(ascending=False).reset_index()
    total = grp["imp_iniziale_eur"].sum()
    grp["cum_perc"] = (grp["imp_iniziale_eur"].cumsum() / total * 100).round(2)
    grp["rank"] = range(1, len(grp) + 1)
    grp["imp_iniziale_eur"] = grp["imp_iniziale_eur"].round(2)
    return grp.to_dict(orient="records")


@app.get("/kpi/saving/per-categoria")
def kpi_saving_per_categoria(
    anno: Optional[int] = Query(None),
    str_ric: Optional[str] = Query(None),
    limit: int = Query(15),
):
    """Breakdown per gruppo merceologico"""
    sb = get_supabase()
    q = sb.table("saving").select("desc_gruppo_merceol,imp_iniziale_eur,saving_eur,negoziazione,str_ric")
    if anno:
        q = q.gte("data_doc", f"{anno}-01-01").lte("data_doc", f"{anno}-12-31")
    if str_ric:
        q = q.eq("str_ric", str_ric)
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return []

    df = df.dropna(subset=["desc_gruppo_merceol"])
    grp = df.groupby("desc_gruppo_merceol").agg(
        impegnato=("imp_iniziale_eur", "sum"),
        saving=("saving_eur", "sum"),
        n_ordini=("imp_iniziale_eur", "count"),
        n_negoziati=("negoziazione", "sum"),
    ).reset_index()
    grp["perc_saving"] = (grp["saving"] / grp["impegnato"] * 100).fillna(0).round(2)
    grp["perc_negoziati"] = (grp["n_negoziati"] / grp["n_ordini"] * 100).fillna(0).round(2)
    top = grp.nlargest(limit, "impegnato")
    return top.to_dict(orient="records")


@app.get("/kpi/saving/valute")
def kpi_valute(anno: Optional[int] = Query(None)):
    """Esposizione valutaria"""
    sb = get_supabase()
    q = sb.table("saving").select("valuta,imp_iniziale_eur,imp_iniziale")
    if anno:
        q = q.gte("data_doc", f"{anno}-01-01").lte("data_doc", f"{anno}-12-31")
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return []

    grp = df.groupby("valuta").agg(
        impegnato_eur=("imp_iniziale_eur", "sum"),
        impegnato_orig=("imp_iniziale", "sum"),
        n_ordini=("imp_iniziale_eur", "count"),
    ).reset_index()
    total = grp["impegnato_eur"].sum()
    grp["perc"] = (grp["impegnato_eur"] / total * 100).round(2)
    return grp.sort_values("impegnato_eur", ascending=False).to_dict(orient="records")


# ─────────────────────────────────────────────
# KPI ENDPOINTS — TEMPI
# ─────────────────────────────────────────────

@app.get("/kpi/tempi/riepilogo")
def kpi_tempi_riepilogo():
    sb = get_supabase()
    rows = sb.table("tempo_attraversamento").select("*").execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return {}

    return {
        "avg_total_days": round(df["total_days"].mean(), 1),
        "avg_purchasing": round(df["days_purchasing"].mean(), 1),
        "avg_auto": round(df["days_auto"].mean(), 1),
        "avg_other": round(df["days_other"].mean(), 1),
        "n_ordini": len(df),
        "perc_bottleneck_purchasing": round(len(df[df["bottleneck"] == "PURCHASING"]) / len(df) * 100, 1),
        "perc_bottleneck_auto": round(len(df[df["bottleneck"] == "AUTO"]) / len(df) * 100, 1),
        "perc_bottleneck_other": round(len(df[df["bottleneck"] == "OTHER"]) / len(df) * 100, 1),
    }


@app.get("/kpi/tempi/mensile")
def kpi_tempi_mensile():
    sb = get_supabase()
    rows = sb.table("tempo_attraversamento").select("*").execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return []

    result = []
    for ym, grp in df.groupby("year_month"):
        result.append({
            "mese": ym,
            "avg_total": round(grp["total_days"].mean(), 1),
            "avg_purchasing": round(grp["days_purchasing"].mean(), 1),
            "avg_auto": round(grp["days_auto"].mean(), 1),
            "avg_other": round(grp["days_other"].mean(), 1),
            "n_ordini": len(grp),
            "n_bottleneck_purchasing": int((grp["bottleneck"] == "PURCHASING").sum()),
            "n_bottleneck_auto": int((grp["bottleneck"] == "AUTO").sum()),
            "n_bottleneck_other": int((grp["bottleneck"] == "OTHER").sum()),
        })
    return sorted(result, key=lambda x: x["mese"])


@app.get("/kpi/tempi/distribuzione")
def kpi_tempi_distribuzione():
    """Distribuzione ordini per fasce di tempo totale"""
    sb = get_supabase()
    rows = sb.table("tempo_attraversamento").select("total_days").execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return []

    bins = [0, 7, 15, 30, 60, 9999]
    labels = ["≤7 gg", "8-15 gg", "16-30 gg", "31-60 gg", ">60 gg"]
    df["fascia"] = pd.cut(df["total_days"], bins=bins, labels=labels, right=True)
    counts = df["fascia"].value_counts().reindex(labels).fillna(0)
    return [{"fascia": k, "n_ordini": int(v)} for k, v in counts.items()]


# ─────────────────────────────────────────────
# KPI ENDPOINTS — NON CONFORMITÀ
# ─────────────────────────────────────────────

@app.get("/kpi/nc/riepilogo")
def kpi_nc_riepilogo():
    sb = get_supabase()
    rows = sb.table("non_conformita").select("non_conformita,delta_giorni,tipo_origine").execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return {}

    n = len(df)
    n_nc = df["non_conformita"].sum()
    return {
        "n_totale": n,
        "n_nc": int(n_nc),
        "perc_nc": round(n_nc / n * 100, 2) if n else 0,
        "avg_delta_giorni": round(df["delta_giorni"].mean(), 1),
        "avg_delta_nc": round(df[df["non_conformita"] == True]["delta_giorni"].mean(), 1),
    }


@app.get("/kpi/nc/mensile")
def kpi_nc_mensile():
    sb = get_supabase()
    rows = sb.table("non_conformita").select("data_origine,non_conformita,delta_giorni,tipo_origine").execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return []

    df["mese"] = pd.to_datetime(df["data_origine"], errors="coerce").dt.strftime("%Y-%m")
    df = df.dropna(subset=["mese"])

    result = []
    for mese, grp in df.groupby("mese"):
        n = len(grp)
        n_nc = grp["non_conformita"].sum()
        result.append({
            "mese": mese,
            "n_totale": n,
            "n_nc": int(n_nc),
            "perc_nc": round(n_nc / n * 100, 2) if n else 0,
            "avg_delta_giorni": round(grp["delta_giorni"].mean(), 1),
        })
    return sorted(result, key=lambda x: x["mese"])


@app.get("/kpi/nc/top-fornitori")
def kpi_nc_top_fornitori(limit: int = Query(10)):
    sb = get_supabase()
    rows = sb.table("non_conformita").select("ragione_sociale,non_conformita,delta_giorni").execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return []

    grp = df.groupby("ragione_sociale").agg(
        n_totale=("non_conformita", "count"),
        n_nc=("non_conformita", "sum"),
        avg_delta=("delta_giorni", "mean"),
    ).reset_index()
    grp["perc_nc"] = (grp["n_nc"] / grp["n_totale"] * 100).round(2)
    grp["avg_delta"] = grp["avg_delta"].round(1)
    top = grp[grp["n_nc"] > 0].nlargest(limit, "n_nc")
    return top.to_dict(orient="records")


@app.get("/kpi/nc/per-tipo")
def kpi_nc_per_tipo():
    sb = get_supabase()
    rows = sb.table("non_conformita").select("tipo_origine,non_conformita,delta_giorni").execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return []

    result = []
    for tipo, grp in df.groupby("tipo_origine"):
        n = len(grp)
        n_nc = grp["non_conformita"].sum()
        result.append({
            "tipo": tipo,
            "n_totale": n,
            "n_nc": int(n_nc),
            "perc_nc": round(n_nc / n * 100, 2) if n else 0,
            "avg_delta_giorni": round(grp["delta_giorni"].mean(), 1),
        })
    return result


# ─────────────────────────────────────────────
# UTILITY
# ─────────────────────────────────────────────

@app.get("/upload/log")
def get_upload_log():
    sb = get_supabase()
    rows = sb.table("upload_log").select("*").order("upload_date", desc=True).limit(50).execute().data
    return rows


@app.delete("/upload/{upload_id}")
def delete_upload(upload_id: str):
    """Cancella un upload e tutti i dati correlati (cascade)"""
    sb = get_supabase()
    sb.table("upload_log").delete().eq("id", upload_id).execute()
    return {"status": "deleted"}


@app.get("/health")
def health():
    return {"status": "ok"}
