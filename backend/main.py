"""
main.py — UA Dashboard Enterprise API v2.1
Fondazione Telethon ETS — Ufficio Acquisti

KPI engine centralizzato. Unica fonte di verità: engines/canonical.py
"""
import io, json, logging, os, re, warnings
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, File, HTTPException, Query, UploadFile, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from supabase import create_client

warnings.filterwarnings("ignore")
load_dotenv()

from engines.canonical import (
    normalize_saving, normalize_vis, calc_kpi,
    TEAM_UA, AREA_MAP, derive_cdc, DOC_LABELS, DOC_NEGOTIABLE,
    USERNAME_MAP, normalize_buyer_name,
)

log = logging.getLogger("ua")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

app = FastAPI(title="UA Dashboard Enterprise", version="2.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ALLOWED_ORIGINS","http://localhost:5173").split(","),
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# ── DB ────────────────────────────────────────────────────────────────────────
def sb(): return create_client(os.getenv("SUPABASE_URL",""), os.getenv("SUPABASE_SERVICE_KEY",""))

PAGE = 2000
MESI = {1:"Gen",2:"Feb",3:"Mar",4:"Apr",5:"Mag",6:"Giu",7:"Lug",8:"Ago",9:"Set",10:"Ott",11:"Nov",12:"Dic"}
GRAN = {
    "mensile":    [(m,m,f"M{m:02d}") for m in range(1,13)],
    "quarter":    [(1,3,"Q1"),(4,6,"Q2"),(7,9,"Q3"),(10,12,"Q4")],
    "semestrale": [(1,6,"S1"),(7,12,"S2")],
    "annuale":    [(1,12,"Anno")],
}

def _query(table, filters=None, select="*"):
    """Query Supabase con paginazione automatica."""
    client = sb(); all_rows = []; offset = 0
    while True:
        q = client.table(table).select(select)
        if filters:
            for k, v in filters.items():
                if v is None or v == "": continue
                if k == "anno": q = q.gte("data_doc", f"{v}-01-01").lte("data_doc", f"{v}-12-31")
                elif k == "str_ric": q = q.eq("str_ric", v)
                elif k == "cdc": q = q.eq("cdc", v)
                elif k == "alfa": q = q.eq("alfa_documento", v)
        batch = q.range(offset, offset+PAGE-1).execute().data
        all_rows.extend(batch); 
        if len(batch) < PAGE: break
        offset += PAGE
    return all_rows

def get_saving_df(filters: dict = None) -> pd.DataFrame:
    """Carica DataFrame saving dal DB normalizzato."""
    rows = _query("saving", filters)
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    for c in ["imp_listino_eur","imp_impegnato_eur","saving_eur","imp_listino_val","imp_impegnato_val","saving_val"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["data_doc"] = pd.to_datetime(df.get("data_doc"), errors="coerce")
    if "negoziazione" in df.columns: df["negoziazione"] = df["negoziazione"].fillna(False).astype(bool)
    if "accred_albo"  in df.columns: df["accred_albo"]  = df["accred_albo"].fillna(False).astype(bool)
    # Rename per calc_kpi
    rename = {"imp_listino_eur":"listino_eur","imp_impegnato_eur":"impegnato_eur","saving_eur":"saving_eur"}
    for old, new in rename.items():
        if old in df.columns and new not in df.columns: df[new] = df[old]
    if "alfa_documento" in df.columns:
        df["is_negotiable"] = df["alfa_documento"].isin(DOC_NEGOTIABLE)
    return df

def _safe_list(v):
    if isinstance(v, list): return v
    if isinstance(v, str):
        try: p = json.loads(v); return p if isinstance(p, list) else []
        except: return []
    return []

# ── SISTEMA ───────────────────────────────────────────────────────────────────
@app.get("/wake")
def wake(): return {"ok": True, "version": "2.1.0"}

@app.get("/health")
def health():
    try: sb().table("upload_log").select("id").limit(1).execute(); db = "reachable"
    except: db = "unreachable"
    return {"status":"ok" if db=="reachable" else "degraded", "database":db, "version":"2.1.0"}

# ── KPI SAVING ────────────────────────────────────────────────────────────────
@app.get("/kpi/saving/anni")
def get_anni():
    try:
        rows = sb().table("saving").select("data_doc").execute().data
        df = pd.DataFrame(rows)
        if df.empty: return []
        anni = sorted(pd.to_datetime(df["data_doc"]).dt.year.dropna().unique().astype(int).tolist(), reverse=True)
        return [{"anno": a} for a in anni]
    except Exception as e:
        log.error(f"get_anni: {e}"); return []

@app.get("/kpi/saving/riepilogo")
def kpi_riepilogo(anno:Optional[int]=Query(None), str_ric:Optional[str]=Query(None),
                   cdc:Optional[str]=Query(None), alfa:Optional[str]=Query(None)):
    try:
        df = get_saving_df({"anno":anno,"str_ric":str_ric,"cdc":cdc,"alfa":alfa})
        return calc_kpi(df)
    except Exception as e:
        log.error(f"kpi_riepilogo: {e}", exc_info=True); raise HTTPException(500,"Errore KPI riepilogo.")

@app.get("/kpi/saving/per-area")
def kpi_per_area(anno:Optional[int]=Query(None)):
    """KPI separati RICERCA e STRUTTURA."""
    try:
        ric  = calc_kpi(get_saving_df({"anno":anno,"str_ric":"RICERCA"}))
        str_ = calc_kpi(get_saving_df({"anno":anno,"str_ric":"STRUTTURA"}))
        return {"RICERCA": ric, "STRUTTURA": str_}
    except Exception as e:
        log.error(f"kpi_per_area: {e}", exc_info=True); raise HTTPException(500,"Errore KPI per area.")

@app.get("/kpi/saving/mensile")
def kpi_mensile(anno:Optional[int]=Query(None), str_ric:Optional[str]=Query(None), cdc:Optional[str]=Query(None)):
    try:
        df = get_saving_df({"anno":anno,"str_ric":str_ric,"cdc":cdc})
        if df.empty: return []
        df["mese"] = df["data_doc"].dt.strftime("%Y-%m")
        result = []
        for m, g in df.groupby("mese"):
            k = calc_kpi(g); k["mese"] = m; result.append(k)
        return sorted(result, key=lambda x: x["mese"])
    except Exception as e:
        log.error(f"kpi_mensile: {e}", exc_info=True); return []

@app.get("/kpi/saving/mensile-con-area")
def kpi_mensile_area(anno:Optional[int]=Query(None), cdc:Optional[str]=Query(None)):
    try:
        df = get_saving_df({"anno":anno,"cdc":cdc})
        if df.empty: return []
        df["mese"] = df["data_doc"].dt.strftime("%Y-%m")
        result = []
        for mese, grp in df.groupby("mese"):
            r = {"mese":mese,"label":MESI.get(int(mese.split("-")[1]),mese)}
            r.update({f"tot_{k}":v for k,v in calc_kpi(grp).items()})
            if "str_ric" in grp.columns:
                ric  = grp[grp["str_ric"]=="RICERCA"]
                str_ = grp[grp["str_ric"]=="STRUTTURA"]
                r.update({f"ric_{k}":v for k,v in calc_kpi(ric).items()})
                r.update({f"str_{k}":v for k,v in calc_kpi(str_).items()})
            result.append(r)
        return sorted(result, key=lambda x: x["mese"])
    except Exception as e:
        log.error(f"kpi_mensile_area: {e}", exc_info=True); return []

@app.get("/kpi/saving/per-cdc")
def kpi_per_cdc(anno:Optional[int]=Query(None), str_ric:Optional[str]=Query(None)):
    try:
        df = get_saving_df({"anno":anno,"str_ric":str_ric})
        if df.empty or "cdc" not in df.columns: return []
        result = []
        for cdc, g in df.groupby("cdc"):
            if not cdc: continue
            k = calc_kpi(g); k["cdc"] = cdc; result.append(k)
        return sorted(result, key=lambda x: x["saving"], reverse=True)
    except Exception as e:
        log.error(f"kpi_per_cdc: {e}", exc_info=True); return []

@app.get("/kpi/saving/per-buyer")
def kpi_per_buyer(anno:Optional[int]=Query(None), str_ric:Optional[str]=Query(None), cdc:Optional[str]=Query(None)):
    try:
        df = get_saving_df({"anno":anno,"str_ric":str_ric,"cdc":cdc})
        if df.empty: return []
        buyer_col = next((c for c in ["utente_presentazione","utente"] if c in df.columns), None)
        if not buyer_col: return []
        df["_buyer"] = df[buyer_col].fillna("").str.strip()
        result = []
        for buyer, g in df.groupby("_buyer"):
            if not buyer or buyer.lower() in ("","nan","none","ordini diretti ricerca","ordini diretti"): continue
            k = calc_kpi(g)
            k["utente"] = buyer; k["is_ua"] = buyer in TEAM_UA; k["area"] = AREA_MAP.get(buyer)
            result.append(k)
        return sorted(result, key=lambda x: x["saving"], reverse=True)
    except Exception as e:
        log.error(f"kpi_per_buyer: {e}", exc_info=True); return []

@app.get("/kpi/saving/per-alfa-documento")
def kpi_per_alfa(anno:Optional[int]=Query(None), str_ric:Optional[str]=Query(None), cdc:Optional[str]=Query(None)):
    try:
        df = get_saving_df({"anno":anno,"str_ric":str_ric,"cdc":cdc})
        if df.empty: return []
        result = []
        for alfa, g in df.groupby("alfa_documento"):
            if not alfa: continue
            k = calc_kpi(g); k["alfa_documento"] = alfa; k["doc_label"] = DOC_LABELS.get(alfa, alfa)
            result.append(k)
        return sorted(result, key=lambda x: x["listino"], reverse=True)
    except Exception as e:
        log.error(f"kpi_per_alfa: {e}", exc_info=True); return []

@app.get("/kpi/saving/per-macro-categoria")
def kpi_per_macro(anno:Optional[int]=Query(None), str_ric:Optional[str]=Query(None), cdc:Optional[str]=Query(None)):
    try:
        df = get_saving_df({"anno":anno,"str_ric":str_ric,"cdc":cdc})
        if df.empty or "macro_categoria" not in df.columns: return []
        df = df[df["macro_categoria"].fillna("") != ""]
        result = []
        for m, g in df.groupby("macro_categoria"):
            k = calc_kpi(g); k["macro_categoria"] = m; result.append(k)
        return sorted(result, key=lambda x: x["saving"], reverse=True)
    except Exception as e:
        log.error(f"kpi_per_macro: {e}", exc_info=True); return []

@app.get("/kpi/saving/per-categoria")
def kpi_per_categoria(anno:Optional[int]=Query(None), str_ric:Optional[str]=Query(None),
                       cdc:Optional[str]=Query(None), limit:int=Query(15)):
    try:
        df = get_saving_df({"anno":anno,"str_ric":str_ric,"cdc":cdc})
        if df.empty or "desc_gruppo_merceol" not in df.columns: return []
        df = df[df["desc_gruppo_merceol"].notna() & (df["desc_gruppo_merceol"] != "")]
        result = []
        for cat, g in df.groupby("desc_gruppo_merceol"):
            k = calc_kpi(g); k["desc_gruppo_merceol"] = cat; result.append(k)
        return sorted(result, key=lambda x: x["saving"], reverse=True)[:limit]
    except Exception as e:
        log.error(f"kpi_per_categoria: {e}", exc_info=True); return []

@app.get("/kpi/saving/top-fornitori")
def kpi_top_fornitori(anno:Optional[int]=Query(None), per:str=Query("saving"),
                       limit:int=Query(20), str_ric:Optional[str]=Query(None), cdc:Optional[str]=Query(None)):
    try:
        df = get_saving_df({"anno":anno,"str_ric":str_ric,"cdc":cdc})
        if df.empty or "ragione_sociale" not in df.columns: return []
        result = []
        for forn, g in df.groupby("ragione_sociale"):
            if not forn or str(forn).strip() in ("","nan","None"): continue
            k = calc_kpi(g); k["ragione_sociale"] = forn
            k["albo"] = bool(g["accred_albo"].mode().iloc[0]) if "accred_albo" in g.columns and len(g) else False
            result.append(k)
        sk = per if per in ("saving","impegnato","listino") else "saving"
        return sorted(result, key=lambda x: x.get(sk,0), reverse=True)[:limit]
    except Exception as e:
        log.error(f"kpi_top_fornitori: {e}", exc_info=True); return []

@app.get("/kpi/saving/pareto-fornitori")
def kpi_pareto(anno:Optional[int]=Query(None), str_ric:Optional[str]=Query(None)):
    try:
        df = get_saving_df({"anno":anno,"str_ric":str_ric})
        if df.empty or "ragione_sociale" not in df.columns: return []
        grp = df.groupby("ragione_sociale")["impegnato_eur"].sum().sort_values(ascending=False).reset_index()
        total = grp["impegnato_eur"].sum()
        if not total: return []
        grp["cum_perc"] = (grp["impegnato_eur"].cumsum()/total*100).round(2)
        grp["rank"] = range(1, len(grp)+1)
        grp.rename(columns={"impegnato_eur":"imp_impegnato_eur"}, inplace=True)
        return grp.to_dict(orient="records")
    except Exception as e:
        log.error(f"kpi_pareto: {e}", exc_info=True); return []

@app.get("/kpi/saving/concentration-index")
def kpi_concentration(anno:Optional[int]=Query(None), str_ric:Optional[str]=Query(None)):
    try:
        df = get_saving_df({"anno":anno,"str_ric":str_ric})
        if df.empty: return {}
        total = float(df["impegnato_eur"].fillna(0).sum())
        if not total: return {}
        grp = df.groupby("ragione_sociale")["impegnato_eur"].sum().sort_values(ascending=False).reset_index()
        grp["share"] = (grp["impegnato_eur"]/total*100).round(2)
        n = len(grp)
        def cs(k): return round(float(grp.head(k)["share"].sum()),2) if k<=n else 100.0
        hhi = round(float((grp["share"]**2).sum()),1)
        return {"n_fornitori_totali":n,"total_impegnato":round(total,2),
                "share_top_5":cs(5),"share_top_10":cs(10),"share_top_20":cs(20),
                "hhi":hhi,"hhi_interpretation":(
                    "Mercato molto concentrato" if hhi>2500 else
                    "Mercato concentrato" if hhi>1500 else
                    "Mercato moderatamente concentrato" if hhi>1000 else
                    "Mercato non concentrato")}
    except Exception as e:
        log.error(f"kpi_concentration: {e}", exc_info=True); return {}

@app.get("/kpi/saving/valute")
def kpi_valute(anno:Optional[int]=Query(None)):
    """Esposizione valutaria — usa valori in valuta ORIGINALE (non EUR)."""
    try:
        df = get_saving_df({"anno":anno})
        if df.empty or "valuta" not in df.columns: return []
        # Usa colonne in valuta originale per l'esposizione valutaria
        lst_col = "imp_listino_val" if "imp_listino_val" in df.columns else "imp_listino_eur"
        imp_col = "imp_impegnato_val" if "imp_impegnato_val" in df.columns else "imp_impegnato_eur"
        sav_col = "saving_val" if "saving_val" in df.columns else "saving_eur"
        grp = df.groupby("valuta").agg(
            impegnato=(imp_col,"sum"), listino=(lst_col,"sum"),
            saving=(sav_col,"sum"), n_ordini=(imp_col,"count"),
        ).reset_index()
        total_eur = float(df["impegnato_eur"].fillna(0).sum())
        grp_eur = df.groupby("valuta")["impegnato_eur"].sum().reset_index().rename(columns={"impegnato_eur":"impegnato_eur"})
        grp = grp.merge(grp_eur, on="valuta", how="left")
        grp["perc"] = (grp["impegnato_eur"]/total_eur*100).round(2) if total_eur else 0
        return grp.sort_values("impegnato_eur", ascending=False).to_dict(orient="records")
    except Exception as e:
        log.error(f"kpi_valute: {e}", exc_info=True); return []

@app.get("/kpi/saving/yoy-granulare")
def kpi_yoy(anno:int=Query(...), granularita:str=Query("mensile"),
             str_ric:Optional[str]=Query(None), cdc:Optional[str]=Query(None)):
    try:
        ap = anno - 1
        periodi = GRAN.get(granularita, GRAN["mensile"])
        df_c = get_saving_df({"anno":anno,"str_ric":str_ric,"cdc":cdc})
        df_p = get_saving_df({"anno":ap,"str_ric":str_ric,"cdc":cdc})

        def _mn(df):
            if df.empty: return df
            df = df.copy(); df["mn"] = df["data_doc"].dt.month; return df

        df_c = _mn(df_c); df_p = _mn(df_p)
        mese_max    = int(df_c["mn"].max()) if not df_c.empty else 0
        ult_giorno  = int(df_c[df_c["mn"]==mese_max]["data_doc"].dt.day.max()) if mese_max and not df_c.empty else 0

        def delta(c, p): return round((c-p)/abs(p)*100,1) if p else None

        chart = []
        for m1, m2, lbl in periodi:
            gc = df_c[(df_c["mn"]>=m1)&(df_c["mn"]<=m2)] if not df_c.empty else pd.DataFrame()
            gp = df_p[(df_p["mn"]>=m1)&(df_p["mn"]<=m2)] if not df_p.empty else pd.DataFrame()
            if len(gc)==0 and len(gp)==0: continue
            parziale = len(gc)>0 and mese_max<m2
            label = MESI.get(m1,lbl) if granularita=="mensile" else lbl
            kc, kp = calc_kpi(gc), calc_kpi(gp)
            chart.append({
                "label":label,"m_start":m1,"m_end":m2,"parziale":parziale,
                f"saving_{anno}":kc["saving"],f"impegnato_{anno}":kc["impegnato"],
                f"listino_{anno}":kc["listino"],f"perc_saving_{anno}":kc["perc_saving"],
                f"saving_{ap}":kp["saving"],f"impegnato_{ap}":kp["impegnato"],
                f"listino_{ap}":kp["listino"],f"perc_saving_{ap}":kp["perc_saving"],
                "ha_dati_curr":len(gc)>0,"ha_dati_prev":len(gp)>0,
                "delta_saving":delta(kc["saving"],kp["saving"]) if not parziale else None,
            })

        mesi_ok = {m for r in chart for m in range(r["m_start"],r["m_end"]+1)
                   if not r["parziale"] and r["ha_dati_curr"] and r["ha_dati_prev"]}
        kc_hl = calc_kpi(df_c[df_c["mn"].isin(mesi_ok)] if not df_c.empty and mesi_ok else df_c)
        kp_hl = calc_kpi(df_p[df_p["mn"].isin(mesi_ok)] if not df_p.empty and mesi_ok else df_p)
        mc = max(mesi_ok) if mesi_ok else mese_max

        nota = ""
        if mese_max and mese_max<12 and not df_c.empty:
            nota = f"Dati {anno} disponibili fino al {df_c['data_doc'].max().date()}."
            if ult_giorno<20 and mese_max>1:
                nota += f" {MESI.get(mese_max,'')} potrebbe essere parziale."

        return {
            "anno":anno,"anno_precedente":ap,"granularita":granularita,
            "chart_data":chart,
            "kpi_headline":{
                "corrente":kc_hl,"precedente":kp_hl,
                "label_curr":f"Gen–{MESI.get(mc,'?')} {anno}",
                "label_prev":f"Gen–{MESI.get(mc,'?')} {ap}",
                "delta":{"listino":delta(kc_hl["listino"],kp_hl["listino"]),
                         "impegnato":delta(kc_hl["impegnato"],kp_hl["impegnato"]),
                         "saving":delta(kc_hl["saving"],kp_hl["saving"]),
                         "perc_saving":round(kc_hl["perc_saving"]-kp_hl["perc_saving"],2) if kp_hl["perc_saving"] else None,
                         "perc_negoziati":delta(kc_hl["perc_negoziati"],kp_hl["perc_negoziati"])},
            },
            "nota":nota,"mese_max":mese_max,"ultimo_giorno":ult_giorno,
        }
    except Exception as e:
        log.error(f"kpi_yoy anno={anno}: {e}", exc_info=True)
        raise HTTPException(500,"Errore calcolo YoY. Verifica che i dati siano stati importati.")

@app.get("/kpi/saving/yoy-cdc")
def kpi_yoy_cdc(anno:int=Query(...)):
    try:
        ap = anno-1
        df_c = get_saving_df({"anno":anno}); df_p = get_saving_df({"anno":ap})
        def by_cdc(df):
            if df.empty: return {}
            return {c:calc_kpi(g) for c,g in df.groupby("cdc") if c}
        curr, prev = by_cdc(df_c), by_cdc(df_p)
        return [{"cdc":c,
                 f"saving_{anno}":curr.get(c,{}).get("saving",0),
                 f"saving_{ap}":prev.get(c,{}).get("saving",0),
                 f"impegnato_{anno}":curr.get(c,{}).get("impegnato",0),
                 f"impegnato_{ap}":prev.get(c,{}).get("impegnato",0),
                 } for c in sorted(set(list(curr)+list(prev)))]
    except Exception as e:
        log.error(f"kpi_yoy_cdc: {e}", exc_info=True); return []

@app.get("/kpi/saving/per-commessa")
def kpi_per_commessa(anno:Optional[int]=Query(None), cdc:Optional[str]=Query(None), limit:int=Query(20)):
    try:
        df = get_saving_df({"anno":anno,"str_ric":"RICERCA","cdc":cdc})
        if df.empty or "prefisso_commessa" not in df.columns: return []
        df = df.dropna(subset=["prefisso_commessa"])
        result = []
        for pref, g in df.groupby("prefisso_commessa"):
            k = calc_kpi(g); k["prefisso_commessa"] = pref; result.append(k)
        return sorted(result, key=lambda x: x["saving"], reverse=True)[:limit]
    except Exception as e:
        log.error(f"kpi_per_commessa: {e}", exc_info=True); return []

@app.get("/filtri/disponibili")
def filtri_disponibili(anno:Optional[int]=Query(None)):
    try:
        df = get_saving_df({"anno":anno})
        if df.empty: return {k:[] for k in ["cdc","str_ric","alfa_documento","macro_categoria","valuta"]}
        def uniq(col):
            if col not in df.columns: return []
            return sorted([str(v) for v in df[col].dropna().unique() if str(v).strip() not in ("","nan")])
        return {"cdc":uniq("cdc"),"str_ric":uniq("str_ric"),
                "alfa_documento":uniq("alfa_documento"),"macro_categoria":uniq("macro_categoria"),
                "valuta":uniq("valuta")}
    except Exception as e:
        log.error(f"filtri_disponibili: {e}", exc_info=True); return {}

# ── TEMPI ─────────────────────────────────────────────────────────────────────
def get_tempi_df():
    try:
        rows = sb().table("tempo_attraversamento").select("*").execute().data
        df = pd.DataFrame(rows)
        for c in ["total_days","days_purchasing","days_auto","days_other"]:
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        return df
    except: return pd.DataFrame()

@app.get("/kpi/tempi/riepilogo")
def kpi_tempi_riepilogo():
    try:
        df = get_tempi_df()
        if df.empty: return {}
        n = len(df)
        def sp(a,b): return round(a/b*100,2) if b else 0
        return {"avg_total_days":round(float(df["total_days"].mean()),1),
                "avg_purchasing":round(float(df["days_purchasing"].mean()),1),
                "avg_auto":round(float(df["days_auto"].mean()),1),
                "n_ordini":n,
                "perc_bottleneck_purchasing":sp(int((df.get("bottleneck",pd.Series(dtype=str))=="PURCHASING").sum()),n)}
    except Exception as e: log.error(f"kpi_tempi_riepilogo: {e}"); return {}

@app.get("/kpi/tempi/mensile")
def kpi_tempi_mensile():
    try:
        df = get_tempi_df()
        if df.empty or "year_month" not in df.columns: return []
        return sorted([{"mese":ym,"avg_total":round(float(g["total_days"].mean()),1),
                        "avg_purchasing":round(float(g["days_purchasing"].mean()),1),"n_ordini":len(g)}
                       for ym,g in df.groupby("year_month")], key=lambda x: x["mese"])
    except Exception as e: log.error(f"kpi_tempi_mensile: {e}"); return []

@app.get("/kpi/tempi/distribuzione")
def kpi_tempi_dist():
    try:
        df = get_tempi_df()
        if df.empty: return []
        bins=[0,7,15,30,60,9999]; labels=["≤7 gg","8–15 gg","16–30 gg","31–60 gg",">60 gg"]
        df["fascia"] = pd.cut(df["total_days"], bins=bins, labels=labels, right=True)
        return [{"fascia":k,"n_ordini":int(v)} for k,v in df["fascia"].value_counts().reindex(labels).fillna(0).items()]
    except Exception as e: log.error(f"kpi_tempi_dist: {e}"); return []

# ── NC ────────────────────────────────────────────────────────────────────────
def get_nc_df():
    try:
        rows = sb().table("non_conformita").select("*").execute().data
        df = pd.DataFrame(rows)
        if "non_conformita" in df.columns: df["non_conformita"] = df["non_conformita"].fillna(False).astype(bool)
        if "delta_giorni" in df.columns: df["delta_giorni"] = pd.to_numeric(df["delta_giorni"], errors="coerce").fillna(0)
        return df
    except: return pd.DataFrame()

def sp(a,b): return round(a/b*100,2) if b else 0

@app.get("/kpi/nc/riepilogo")
def kpi_nc_riepilogo():
    try:
        df = get_nc_df()
        if df.empty: return {}
        n=len(df); nnc=int(df["non_conformita"].sum())
        return {"n_totale":n,"n_nc":nnc,"perc_nc":sp(nnc,n),
                "avg_delta_giorni":round(float(df["delta_giorni"].mean()),1),
                "avg_delta_nc":round(float(df[df["non_conformita"]]["delta_giorni"].mean()),1) if nnc else 0}
    except Exception as e: log.error(f"kpi_nc_riepilogo: {e}"); return {}

@app.get("/kpi/nc/mensile")
def kpi_nc_mensile():
    try:
        df = get_nc_df()
        if df.empty: return []
        df["mese"] = pd.to_datetime(df.get("data_origine"), errors="coerce").dt.strftime("%Y-%m")
        df = df.dropna(subset=["mese"])
        return sorted([{"mese":m,"n_totale":len(g),"n_nc":int(g["non_conformita"].sum()),
                        "perc_nc":sp(int(g["non_conformita"].sum()),len(g))}
                       for m,g in df.groupby("mese")], key=lambda x: x["mese"])
    except Exception as e: log.error(f"kpi_nc_mensile: {e}"); return []

@app.get("/kpi/nc/top-fornitori")
def kpi_nc_top(limit:int=Query(10)):
    try:
        df = get_nc_df()
        if df.empty: return []
        grp = df.groupby("ragione_sociale").agg(n_totale=("non_conformita","count"),
            n_nc=("non_conformita","sum"),avg_delta=("delta_giorni","mean")).reset_index()
        grp["perc_nc"] = (grp["n_nc"]/grp["n_totale"]*100).round(2)
        return grp[grp["n_nc"]>0].nlargest(limit,"n_nc").to_dict(orient="records")
    except Exception as e: log.error(f"kpi_nc_top: {e}"); return []

@app.get("/kpi/nc/per-tipo")
def kpi_nc_tipo():
    try:
        df = get_nc_df()
        if df.empty or "tipo_origine" not in df.columns: return []
        return [{"tipo":t,"n_totale":len(g),"n_nc":int(g["non_conformita"].sum()),
                 "perc_nc":sp(int(g["non_conformita"].sum()),len(g))} for t,g in df.groupby("tipo_origine")]
    except Exception as e: log.error(f"kpi_nc_tipo: {e}"); return []

# ── RISORSE ───────────────────────────────────────────────────────────────────
def get_risorse_df():
    try:
        rows = sb().table("resource_performance").select("*").execute().data
        return pd.DataFrame(rows)
    except: return pd.DataFrame()

@app.get("/kpi/risorse/riepilogo")
def kpi_risorse_riepilogo():
    try:
        df = get_risorse_df()
        if df.empty: return {"available":False,"reason":"Nessun file risorse caricato."}
        for c in ["pratiche_gestite","saving_generato"]:
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        return {"available":True,"n_record":len(df),
                "n_risorse":df["risorsa"].dropna().nunique() if "risorsa" in df.columns else 0,
                "tot_saving":round(float(df["saving_generato"].sum()),2) if "saving_generato" in df.columns else 0,
                "avg_pratiche":round(float(df["pratiche_gestite"].mean()),1) if "pratiche_gestite" in df.columns else 0}
    except Exception as e: log.error(f"kpi_risorse_riepilogo: {e}"); return {"available":False,"reason":str(e)}

@app.get("/kpi/risorse/per-risorsa")
def kpi_risorse_per_risorsa(anno:Optional[int]=Query(None)):
    try:
        df = get_risorse_df()
        if df.empty: return []
        if anno and "year" in df.columns: df = df[df["year"]==anno]
        result = []
        for risorsa, g in df.groupby("risorsa"):
            for c in ["pratiche_gestite","saving_generato","negoziazioni_concluse","tempo_medio_giorni"]:
                if c in g.columns: g[c] = pd.to_numeric(g[c], errors="coerce").fillna(0)
            result.append({"risorsa":risorsa,
                "struttura":g["struttura"].dropna().mode().iloc[0] if "struttura" in g.columns and not g["struttura"].dropna().empty else None,
                "pratiche_gestite":int(g.get("pratiche_gestite",pd.Series([0])).sum()),
                "saving_generato":round(float(g.get("saving_generato",pd.Series([0])).sum()),2),
                "tempo_medio_giorni":round(float(g["tempo_medio_giorni"].mean()),1) if "tempo_medio_giorni" in g.columns and not g["tempo_medio_giorni"].dropna().empty else None})
        return sorted(result, key=lambda x: x["saving_generato"], reverse=True)
    except Exception as e: log.error(f"kpi_risorse_per_risorsa: {e}"); return []

@app.get("/kpi/risorse/mensile")
def kpi_risorse_mensile(anno:Optional[int]=Query(None)):
    try:
        df = get_risorse_df()
        if df.empty: return []
        if anno and "year" in df.columns: df = df[df["year"]==anno]
        result = []
        for m, g in df.groupby("mese_label"):
            result.append({"mese":m,
                "pratiche_totali":int(g.get("pratiche_gestite",pd.Series([0])).sum()),
                "saving_totale":round(float(g.get("saving_generato",pd.Series([0])).sum()),2),
                "n_risorse":g["risorsa"].nunique() if "risorsa" in g.columns else 0})
        return sorted(result, key=lambda x: x["mese"])
    except Exception as e: log.error(f"kpi_risorse_mensile: {e}"); return []

# ── UPLOAD ────────────────────────────────────────────────────────────────────
def detect_file_type(filename: str):
    fn = filename.lower()
    m = re.search(r'20(\d{2})', fn)
    anno = int("20" + m.group(1)) if m else None
    if "vis_dettagliata" in fn or "estrazione_dettagliata" in fn: return "ordini_dettaglio", anno
    if "saving" in fn or "file_saving" in fn: return "saving", anno
    if "risorse" in fn or "resource" in fn: return "risorse", anno
    if "tempi" in fn or "throughput" in fn: return "tempi", anno
    if "nc" in fn or "non_conformita" in fn: return "nc", anno
    return "unknown", anno

@app.post("/upload/inspect")
async def upload_inspect(file: UploadFile = File(...)):
    contents = await file.read()
    file_type, anno = detect_file_type(file.filename)
    try:
        df = pd.read_excel(io.BytesIO(contents), nrows=5)
        cols = list(df.columns)
        has_saving = any(c in cols for c in ["Imp.iniziale","Imp. Iniziale €","Imp.negoziato"])
        has_vis    = any(c in cols for c in ["Cod. documento","Importo riga","Protocollo Ordine"])
        if file_type=="unknown":
            file_type = "saving" if has_saving else "ordini_dettaglio" if has_vis else "unknown"
        label_map = {"saving":"File Saving/Ordini","ordini_dettaglio":"Estrazione Dettagliata",
                     "risorse":"File Risorse/Team","tempi":"Tempi Attraversamento","nc":"Non Conformità"}
        return {"filename":file.filename,"file_type":file_type,"anno":anno,"n_columns":len(cols),
                "sample_columns":cols[:15],"can_proceed":file_type!="unknown",
                "family":file_type,"family_label":label_map.get(file_type,"Sconosciuto"),
                "available_analyses":["KPI Riepilogo","Saving YoY","Per CDC","Per Buyer"] if has_saving else [],
                "blocked_analyses":[],"warnings":[],"year_detected":anno,"years_found":[anno] if anno else [],
                "yoy_ready":bool(anno),"yoy_note":f"Anno {anno} rilevato." if anno else "Anno non rilevato.",
                "normalization_notes":[]}
    except Exception as e:
        raise HTTPException(400, f"Errore ispezione: {str(e)[:200]}")

@app.post("/upload/auto")
async def upload_auto(file: UploadFile = File(...), cdc_override:Optional[str]=Query(None)):
    contents = await file.read()
    file_type, anno = detect_file_type(file.filename)
    if file_type=="saving" and anno:
        return await _upload_saving(contents, file.filename, anno, cdc_override)
    elif file_type=="ordini_dettaglio":
        return await _upload_vis(contents, file.filename)
    elif file_type=="risorse":
        return await _upload_risorse(contents, file.filename)
    elif file_type=="tempi":
        return await _upload_tempi(contents, file.filename)
    elif file_type=="nc":
        return await _upload_nc(contents, file.filename)
    else:
        # Auto-detect dal contenuto
        try:
            df = pd.read_excel(io.BytesIO(contents), nrows=3)
            if any(c in df.columns for c in ["Imp.iniziale","Imp. Iniziale €"]):
                anno = anno or 2026
                return await _upload_saving(contents, file.filename, anno, cdc_override)
            elif "Importo riga" in df.columns:
                return await _upload_vis(contents, file.filename)
        except: pass
        raise HTTPException(400,f"Tipo file non riconoscibile da '{file.filename}'. Usa: saving_2026_*.xlsx o vis_dettagliata_*.xlsx")

async def _upload_saving(contents, filename, anno, cdc_override=None):
    try: df_raw = pd.read_excel(io.BytesIO(contents))
    except Exception as e: raise HTTPException(400,f"Errore lettura: {e}")

    df = normalize_saving(df_raw, anno)
    client = sb()
    # Pulisci anno precedente
    try: client.table("saving").delete().filter("data_doc","gte",f"{anno}-01-01").filter("data_doc","lte",f"{anno}-12-31").execute()
    except Exception as e: log.warning(f"Pulizia anno {anno}: {e}")

    upload_id = None
    try:
        lr = client.table("upload_log").insert({"filename":filename,"tipo":"saving","family_detected":"savings",
            "mapping_confidence":"high","mapping_score":0.95,
            "available_analyses":["KPI Riepilogo","Saving YoY","Per CDC","Per Buyer","Top Fornitori"],
            "blocked_analyses":[],"warnings":[]}).execute()
        upload_id = lr.data[0]["id"]
    except Exception as e: log.error(f"Log upload: {e}")

    records = []
    for _, row in df.iterrows():
        if pd.isna(row.get("data_doc")): continue
        rec = {
            "upload_id":upload_id,
            "data_doc":str(row["data_doc"].date()) if pd.notna(row.get("data_doc")) else None,
            "alfa_documento":row.get("alfa_documento") or None,
            "str_ric":row.get("str_ric") or None,
            "ragione_sociale":row.get("fornitore") or None,
            "accred_albo":bool(row.get("accred_albo",False)),
            "cdc":cdc_override or row.get("cdc") or None,
            "centro_di_costo":row.get("centro_costo") or None,
            "desc_cdc":row.get("desc_cdc") or None,
            "utente":row.get("buyer_raw") or None,
            "utente_presentazione":row.get("buyer") or None,
            "protoc_ordine":str(row["protoc_ordine"]) if pd.notna(row.get("protoc_ordine")) else None,
            "protoc_commessa":str(row["protoc_commessa"]) if pd.notna(row.get("protoc_commessa")) else None,
            "desc_gruppo_merceol":row.get("desc_gruppo") or None,
            "macro_categoria":row.get("macro_cat") or None,
            "valuta":row.get("valuta") or "EURO",
            # EUR (per analytics)
            "imp_listino_eur":float(row.get("listino_eur",0)),
            "imp_impegnato_eur":float(row.get("impegnato_eur",0)),
            "saving_eur":float(row.get("saving_eur",0)),
            "perc_saving_eur":float(row.get("perc_saving",0)),
            # Valuta originale (per esposizione valutaria)
            "imp_listino_val":float(row.get("listino_val",0)),
            "imp_impegnato_val":float(row.get("impegnato_val",0)),
            "saving_val":float(row.get("saving_val",0)),
            "negoziazione":bool(row.get("negoziazione",False)),
        }
        records.append({k:(None if (isinstance(v,float) and v!=v) else v) for k,v in rec.items()})

    inserted = 0
    for i in range(0, len(records), 5000):
        try: client.table("saving").insert(records[i:i+5000]).execute(); inserted += min(5000,len(records)-i)
        except Exception as e: log.error(f"Batch saving insert: {str(e)[:200]}")

    kpi = calc_kpi(df)
    return {"status":"ok","upload_id":upload_id,"rows_inserted":inserted,
            "rows_skipped":len(df)-inserted,"anno":anno,"family":"saving",
            "family_label":"File Saving/Ordini",
            "kpi_preview":{"listino":round(kpi["listino"],2),"impegnato":round(kpi["impegnato"],2),
                           "saving":round(kpi["saving"],2),"perc_saving":kpi["perc_saving"]},
            "available_analyses":["KPI Riepilogo","Saving YoY","Per CDC","Per Buyer","Top Fornitori"],
            "blocked_analyses":[],"warnings":[],"year_detected":anno,"years_found":[anno],"yoy_ready":True}

async def _upload_vis(contents, filename):
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        df_raw = None
        for sheet in xl.sheet_names:
            test = pd.read_excel(io.BytesIO(contents), sheet_name=sheet, nrows=3)
            if "Cod. documento" in test.columns or "Importo riga" in test.columns:
                df_raw = pd.read_excel(io.BytesIO(contents), sheet_name=sheet, header=0); break
        if df_raw is None: raise ValueError("Foglio dati non trovato")
    except Exception as e: raise HTTPException(400,f"Errore lettura: {e}")

    df = normalize_vis(df_raw)
    client = sb()
    try: client.table("ordini_dettaglio").delete().neq("id","00000000-0000-0000-0000-000000000000").execute()
    except Exception as e: log.warning(f"Pulizia ordini_dettaglio: {e}")

    upload_id = None
    try:
        lr = client.table("upload_log").insert({"filename":filename,"tipo":"ordini_dettaglio",
            "family_detected":"ordini_dettaglio","mapping_confidence":"high",
            "available_analyses":["Analisi Ordini","Categorie","Articoli","Progetti"],
            "blocked_analyses":[],"warnings":[]}).execute()
        upload_id = lr.data[0]["id"]
    except Exception as e: log.error(f"Log upload vis: {e}")

    records = []
    for _, row in df.iterrows():
        rec = {"upload_id":upload_id,
               "cod_documento":row.get("cod_documento") or None,"doc_label":row.get("doc_label") or None,
               "is_order":bool(row.get("is_order",False)),"is_logistics":bool(row.get("is_logistics",False)),
               "nr_doc":str(row["nr_doc"]) if pd.notna(row.get("nr_doc")) else None,
               "data_doc":str(row["data_doc"].date()) if pd.notna(row.get("data_doc")) else None,
               "fornitore":row.get("fornitore") or None,
               "tot_documento":float(row.get("tot_documento",0)),
               "importo_riga":float(row.get("importo_riga",0)),
               "stato_doc":row.get("stato_doc") or None,"stato_evasione":row.get("stato_evasione") or None,
               "protoc_ordine":str(row["protoc_ordine"]) if pd.notna(row.get("protoc_ordine")) else None,
               "protoc_commessa":str(row["protoc_commessa"]) if pd.notna(row.get("protoc_commessa")) else None,
               "progetto":row.get("progetto") or None,"utente_ins_raw":row.get("utente_ins_raw") or None,
               "utente_ins":row.get("utente_ins") or None,"is_ua":bool(row.get("is_ua",False)),
               "categoria":row.get("categoria") or None,"famiglia":row.get("famiglia") or None,
               "cod_famiglia":row.get("cod_famiglia") or None,"sottofamiglia":row.get("sottofamiglia") or None,
               "cod_articolo":row.get("cod_articolo") or None,"desc_articolo":row.get("desc_articolo") or None,
               "quantita":float(row.get("quantita",0)),"prezzo_unitario":float(row.get("prezzo_unitario",0)),
               "cdc":row.get("cdc") or None,"valuta":row.get("valuta") or "EURO"}
        records.append({k:(None if (isinstance(v,float) and v!=v) else v) for k,v in rec.items()})

    inserted = 0
    try:
        for i in range(0,len(records),2000):
            client.table("ordini_dettaglio").insert(records[i:i+2000]).execute(); inserted+=min(2000,len(records)-i)
    except Exception as e: log.error(f"Batch vis insert: {str(e)[:200]}")

    return {"status":"ok","upload_id":upload_id,"rows_inserted":inserted,"family":"ordini_dettaglio",
            "family_label":"Estrazione Dettagliata Ordini",
            "available_analyses":["Analisi Ordini","Categorie","Articoli","Progetti"],
            "blocked_analyses":[],"warnings":[],"year_detected":None,"years_found":[],"yoy_ready":False}

async def _upload_risorse(contents, filename):
    from upload_engine import inspect_and_load, normalize_risorse_row
    wbi = inspect_and_load(contents, filename); col_map = wbi.mapping_result.fields; client = sb()
    lr = client.table("upload_log").insert({"filename":filename,"tipo":"risorse"}).execute()
    upload_id = lr.data[0]["id"]
    records = [r for _,row in wbi.df.iterrows() if (r:=normalize_risorse_row(col_map,row,upload_id))]
    if records: client.table("resource_performance").insert(records).execute()
    return {"status":"ok","rows_inserted":len(records),"family":"risorse",
            "available_analyses":[],"blocked_analyses":[],"warnings":[],"year_detected":None,"years_found":[],"yoy_ready":False}

async def _upload_tempi(contents, filename):
    from upload_engine import inspect_and_load, normalize_tempi_row
    wbi = inspect_and_load(contents, filename); col_map = wbi.mapping_result.fields; client = sb()
    lr = client.table("upload_log").insert({"filename":filename,"tipo":"tempi"}).execute()
    upload_id = lr.data[0]["id"]
    records = [normalize_tempi_row(col_map,row,upload_id) for _,row in wbi.df.iterrows()]
    if records: client.table("tempo_attraversamento").insert(records).execute()
    return {"status":"ok","rows_inserted":len(records),"family":"tempi",
            "available_analyses":[],"blocked_analyses":[],"warnings":[],"year_detected":None,"years_found":[],"yoy_ready":False}

async def _upload_nc(contents, filename):
    from upload_engine import inspect_and_load, normalize_nc_row
    wbi = inspect_and_load(contents, filename); col_map = wbi.mapping_result.fields; client = sb()
    lr = client.table("upload_log").insert({"filename":filename,"tipo":"nc"}).execute()
    upload_id = lr.data[0]["id"]
    records = [normalize_nc_row(col_map,row,upload_id) for _,row in wbi.df.iterrows()]
    if records: client.table("non_conformita").insert(records).execute()
    return {"status":"ok","rows_inserted":len(records),"family":"nc",
            "available_analyses":[],"blocked_analyses":[],"warnings":[],"year_detected":None,"years_found":[],"yoy_ready":False}

@app.post("/upload/saving")
async def upload_saving_compat(file:UploadFile=File(...), cdc_override:Optional[str]=Query(None)):
    contents = await file.read(); _, anno = detect_file_type(file.filename)
    if not anno: raise HTTPException(400,"Anno non rilevato. Usa: saving_2026_*.xlsx")
    return await _upload_saving(contents, file.filename, anno, cdc_override)

@app.post("/upload/risorse")
async def upload_risorse_compat(file:UploadFile=File(...)): return await _upload_risorse(await file.read(), file.filename)

@app.post("/upload/tempi")
async def upload_tempi_compat(file:UploadFile=File(...)): return await _upload_tempi(await file.read(), file.filename)

@app.post("/upload/nc")
async def upload_nc_compat(file:UploadFile=File(...)): return await _upload_nc(await file.read(), file.filename)

@app.get("/upload/log")
def upload_log():
    try:
        rows = sb().table("upload_log").select("*").order("upload_date",desc=True).limit(50).execute().data
        if not isinstance(rows, list): return []
        for row in rows:
            for f in ["available_analyses","blocked_analyses","warnings"]: row[f] = _safe_list(row.get(f))
        return rows
    except Exception as e: log.error(f"upload_log: {e}"); return []

@app.delete("/upload/{upload_id}")
def delete_upload(upload_id: str):
    try:
        sb().table("saving").delete().eq("upload_id",upload_id).execute()
        sb().table("upload_log").delete().eq("id",upload_id).execute()
        return {"status":"deleted"}
    except Exception as e: raise HTTPException(500,str(e))

# ── EXPORT EXCEL ──────────────────────────────────────────────────────────────
@app.post("/export/custom/excel")
async def export_excel(body:dict=Body(...)):
    filtri = body.get("filtri",{}); anno=filtri.get("anno"); str_ric=filtri.get("str_ric"); cdc=filtri.get("cdc")
    sezioni = body.get("sezioni",["riepilogo","mensile","cdc","top_fornitori","alfa_documento","buyer"])
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        # Foglio 1: KPI Riepilogo
        k = calc_kpi(get_saving_df({"anno":anno,"str_ric":str_ric,"cdc":cdc}))
        pd.DataFrame([
            ["KPI","Valore","Definizione"],
            ["Listino €",k["listino"],"SUM(Imp. Iniziale €) — prezzo base"],
            ["Impegnato €",k["impegnato"],"SUM(Imp. Negoziato €) — quanto paghiamo"],
            ["Saving €",k["saving"],"Listino - Impegnato"],
            ["% Saving",f"{k['perc_saving']}%","Saving/Listino × 100"],
            ["N° Righe",k["n_righe"],"Totale documenti"],
            ["Negoziabili",k["n_negotiable"],"OS/OSP/OPR/ORN/ORD/PS"],
            ["Negoziati",k["n_negoziati"],"Righe con Negoziazione=SI"],
            ["% Negoziati",f"{k['perc_negoziati']}%","Negoziati/Negoziabili × 100"],
            ["Accreditati Albo",k["n_albo"],"Righe con Accred.albo=SI"],
            ["% Albo",f"{k['perc_albo']}%","Accreditati/Totale × 100"],
        ], columns=["KPI","Valore","Definizione"]).to_excel(writer, index=False, sheet_name="KPI Riepilogo")
        # Altri fogli
        defs = [("mensile",kpi_mensile,"Saving Mensile"),("cdc",kpi_per_cdc,"Saving per CDC"),
                ("top_fornitori",lambda:kpi_top_fornitori(anno,"saving",20,str_ric,cdc),"Top 20 Fornitori"),
                ("alfa_documento",lambda:kpi_per_alfa(anno,str_ric,cdc),"Per Tipo Documento"),
                ("buyer",lambda:kpi_per_buyer(anno,str_ric,cdc),"Saving per Buyer")]
        for key, fn, sheet_name in defs:
            if key in sezioni:
                try:
                    if key=="mensile": d = kpi_mensile(anno,str_ric,cdc)
                    elif key=="cdc": d = kpi_per_cdc(anno,str_ric)
                    else: d = fn()
                    if d: pd.DataFrame(d).to_excel(writer, index=False, sheet_name=sheet_name)
                except: pass
    buf.seek(0)
    fn = f"report_{anno or 'tutti'}_{str_ric or 'tutte'}.xlsx"
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition":f"attachment; filename={fn}"})
