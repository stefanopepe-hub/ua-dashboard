"""
routers/analytics.py — Analytics router
HTTP layer puro. Tutta la logica è in services/analytics.py.
"""
import logging
from typing import Optional
from fastapi import APIRouter, Query, HTTPException
from supabase import create_client
import os

from services.analytics import (
    get_anni, kpi_riepilogo, kpi_mensile, kpi_mensile_area,
    kpi_per_cdc, kpi_per_buyer, kpi_per_alfa, kpi_per_macro,
    kpi_per_commessa, kpi_top_fornitori, kpi_pareto,
    kpi_concentration, kpi_valute, kpi_yoy, kpi_yoy_cdc,
    kpi_per_protocollo_commessa, kpi_per_protocollo_ordine, kpi_per_buyer_cdc,
    query, safe_pct,
)
from domain import calc_kpi
import pandas as pd

log = logging.getLogger("ua.analytics")
router = APIRouter(prefix="/kpi", tags=["analytics"])


def sb():
    return create_client(
        os.getenv("SUPABASE_URL", ""),
        os.getenv("SUPABASE_SERVICE_KEY", "")
    )


# ── Saving ───────────────────────────────────────────────────────

@router.get("/saving/anni")
def api_anni():
    return get_anni(sb())

@router.get("/saving/riepilogo")
def api_riepilogo(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None), alfa: Optional[str] = Query(None),
    macro: Optional[str] = Query(None),
):
    return kpi_riepilogo(sb(), anno, str_ric, cdc, alfa, macro)

@router.get("/saving/mensile")
def api_mensile(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None), cdc: Optional[str] = Query(None)):
    return kpi_mensile(sb(), anno, str_ric, cdc)

@router.get("/saving/mensile-con-area")
def api_mensile_area(anno: Optional[int] = Query(None), cdc: Optional[str] = Query(None)):
    return kpi_mensile_area(sb(), anno, cdc)

@router.get("/saving/per-cdc")
def api_per_cdc(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None)):
    return kpi_per_cdc(sb(), anno, str_ric)

@router.get("/saving/per-buyer")
def api_per_buyer(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None), cdc: Optional[str] = Query(None)):
    return kpi_per_buyer(sb(), anno, str_ric, cdc)

@router.get("/saving/per-alfa-documento")
def api_per_alfa(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None), cdc: Optional[str] = Query(None)):
    return kpi_per_alfa(sb(), anno, str_ric, cdc)

@router.get("/saving/per-macro-categoria")
def api_per_macro(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None), cdc: Optional[str] = Query(None)):
    return kpi_per_macro(sb(), anno, str_ric, cdc)

@router.get("/saving/per-commessa")
def api_per_commessa(anno: Optional[int] = Query(None), cdc: Optional[str] = Query(None), limit: int = Query(20)):
    return kpi_per_commessa(sb(), anno, cdc, limit)

@router.get("/saving/top-fornitori")
def api_top_fornitori(
    anno: Optional[int] = Query(None), per: str = Query("saving"),
    limit: int = Query(10), str_ric: Optional[str] = Query(None), cdc: Optional[str] = Query(None),
):
    return kpi_top_fornitori(sb(), anno, per, limit, str_ric, cdc)

@router.get("/saving/pareto-fornitori")
def api_pareto(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None)):
    return kpi_pareto(sb(), anno, str_ric)

@router.get("/saving/concentration-index")
def api_concentration(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None)):
    return kpi_concentration(sb(), anno, str_ric)

@router.get("/saving/valute")
def api_valute(anno: Optional[int] = Query(None)):
    return kpi_valute(sb(), anno)

@router.get("/saving/yoy-granulare")
def api_yoy(anno: int = Query(...), granularita: str = Query("mensile"), str_ric: Optional[str] = Query(None), cdc: Optional[str] = Query(None)):
    return kpi_yoy(sb(), anno, granularita, str_ric, cdc)

@router.get("/saving/yoy-cdc")
def api_yoy_cdc(anno: int = Query(...)):
    return kpi_yoy_cdc(sb(), anno)

@router.get("/saving/per-protocollo-commessa")
def api_proto_comm(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None), cdc: Optional[str] = Query(None), limit: int = Query(20)):
    return kpi_per_protocollo_commessa(sb(), anno, str_ric, cdc, limit)

@router.get("/saving/per-protocollo-ordine")
def api_proto_ord(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None), limit: int = Query(20)):
    return kpi_per_protocollo_ordine(sb(), anno, str_ric, limit)

@router.get("/saving/per-buyer-cdc")
def api_buyer_cdc(anno: Optional[int] = Query(None)):
    return kpi_per_buyer_cdc(sb(), anno)

@router.get("/saving/per-categoria")
def api_per_categoria(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None), cdc: Optional[str] = Query(None), limit: int = Query(15)):
    from services.analytics import get_saving_df
    df = get_saving_df(sb(), anno, str_ric, cdc,
        cols="desc_gruppo_merceol,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df = df.dropna(subset=["desc_gruppo_merceol"])
    result = [{"desc_gruppo_merceol": c, **calc_kpi(g)} for c, g in df.groupby("desc_gruppo_merceol")]
    return sorted(result, key=lambda x: x["saving"], reverse=True)[:limit]


# ── Risorse ──────────────────────────────────────────────────────

@router.get("/risorse/riepilogo")
def api_risorse_riepilogo():
    rows = query(sb(), "resource_performance")
    df = pd.DataFrame(rows)
    if df.empty:
        return {"available": False, "reason": "Nessun file risorse caricato."}
    return {
        "available": True,
        "n_record": len(df),
        "n_risorse": df["risorsa"].dropna().nunique(),
        "avg_pratiche_gestite": round(float(df["pratiche_gestite"].dropna().mean()), 1) if not df["pratiche_gestite"].dropna().empty else 0,
        "tot_saving_generato":  round(float(df["saving_generato"].dropna().sum()),  2),
    }

@router.get("/risorse/per-risorsa")
def api_risorse_per_risorsa(anno: Optional[int] = Query(None)):
    rows = query(sb(), "resource_performance")
    df = pd.DataFrame(rows)
    if df.empty: return []
    if anno: df = df[df["year"] == anno]
    if df.empty: return []
    result = []
    for risorsa, g in df.groupby("risorsa"):
        result.append({
            "risorsa": risorsa,
            "struttura": g["struttura"].dropna().mode().iloc[0] if not g["struttura"].dropna().empty else None,
            "pratiche_gestite":      int(g["pratiche_gestite"].sum()),
            "pratiche_aperte":       int(g["pratiche_aperte"].sum()),
            "pratiche_chiuse":       int(g["pratiche_chiuse"].sum()),
            "saving_generato":       round(float(g["saving_generato"].sum()), 2),
            "negoziazioni_concluse": int(g["negoziazioni_concluse"].sum()),
            "tempo_medio_giorni":    round(float(g["tempo_medio_giorni"].mean()), 1) if not g["tempo_medio_giorni"].dropna().empty else None,
            "efficienza":            round(float(g["efficienza"].mean()), 1) if not g["efficienza"].dropna().empty else None,
        })
    return sorted(result, key=lambda x: x["saving_generato"], reverse=True)

@router.get("/risorse/mensile")
def api_risorse_mensile(anno: Optional[int] = Query(None)):
    rows = query(sb(), "resource_performance")
    df = pd.DataFrame(rows)
    if df.empty: return []
    if anno: df = df[df["year"] == anno]
    if df.empty: return []
    result = [{"mese": m, "pratiche_totali": int(g["pratiche_gestite"].sum()),
               "saving_totale": round(float(g["saving_generato"].sum()), 2),
               "n_risorse_attive": g["risorsa"].nunique()}
              for m, g in df.groupby("mese_label")]
    return sorted(result, key=lambda x: x["mese"])


# ── Tempi ─────────────────────────────────────────────────────────

@router.get("/tempi/riepilogo")
def api_tempi_riepilogo():
    rows = query(sb(), "tempo_attraversamento")
    df = pd.DataFrame(rows)
    if df.empty: return {}
    n = len(df)
    return {
        "avg_total_days": round(float(df["total_days"].mean()), 1),
        "avg_purchasing":  round(float(df["days_purchasing"].mean()), 1),
        "avg_auto":        round(float(df["days_auto"].mean()), 1),
        "avg_other":       round(float(df["days_other"].mean()), 1),
        "n_ordini": n,
        "perc_bottleneck_purchasing": safe_pct(int((df["bottleneck"] == "PURCHASING").sum()), n),
    }

@router.get("/tempi/mensile")
def api_tempi_mensile():
    rows = query(sb(), "tempo_attraversamento")
    df = pd.DataFrame(rows)
    if df.empty: return []
    result = []
    for ym, g in df.groupby("year_month"):
        n = len(g)
        result.append({
            "mese": ym,
            "avg_total": round(float(g["total_days"].mean()), 1),
            "avg_purchasing": round(float(g["days_purchasing"].mean()), 1),
            "avg_auto": round(float(g["days_auto"].mean()), 1),
            "avg_other": round(float(g["days_other"].mean()), 1) if "days_other" in g else 0,
            "n_ordini": n,
            "n_bottleneck_purchasing": int((g["bottleneck"] == "PURCHASING").sum()) if "bottleneck" in g else 0,
            "n_bottleneck_auto": int((g["bottleneck"] == "AUTO").sum()) if "bottleneck" in g else 0,
        })
    return sorted(result, key=lambda x: x["mese"])

@router.get("/tempi/distribuzione")
def api_tempi_dist():
    rows = query(sb(), "tempo_attraversamento", select="total_days")
    df = pd.DataFrame(rows)
    if df.empty: return []
    bins = [0, 7, 15, 30, 60, 9999]
    labels = ["≤7 gg", "8-15 gg", "16-30 gg", "31-60 gg", ">60 gg"]
    df["f"] = pd.cut(df["total_days"], bins=bins, labels=labels, right=True)
    return [{"fascia": k, "n_ordini": int(v)}
            for k, v in df["f"].value_counts().reindex(labels).fillna(0).items()]


# ── NC ────────────────────────────────────────────────────────────

@router.get("/nc/riepilogo")
def api_nc_riepilogo():
    rows = query(sb(), "non_conformita", select="non_conformita,delta_giorni")
    df = pd.DataFrame(rows)
    if df.empty: return {}
    n = len(df); nnc = int(df["non_conformita"].sum())
    df_nc = df[df["non_conformita"] == True]
    return {
        "n_totale": n, "n_nc": nnc,
        "perc_nc": safe_pct(nnc, n),
        "avg_delta_giorni": round(float(df["delta_giorni"].mean()), 1),
        "avg_delta_nc": round(float(df_nc["delta_giorni"].mean()), 1) if len(df_nc) > 0 else 0.0,
    }

@router.get("/nc/mensile")
def api_nc_mensile():
    rows = query(sb(), "non_conformita", select="data_origine,non_conformita,delta_giorni")
    df = pd.DataFrame(rows)
    if df.empty: return []
    df["mese"] = pd.to_datetime(df["data_origine"], errors="coerce").dt.strftime("%Y-%m")
    df = df.dropna(subset=["mese"])
    result = []
    for m, g in df.groupby("mese"):
        n = len(g); nnc = int(g["non_conformita"].sum())
        result.append({"mese": m, "n_totale": n, "n_nc": nnc,
                       "perc_nc": safe_pct(nnc, n),
                       "avg_delta": round(float(g["delta_giorni"].mean()), 1)})
    return sorted(result, key=lambda x: x["mese"])

@router.get("/nc/top-fornitori")
def api_nc_top(limit: int = Query(10)):
    rows = query(sb(), "non_conformita", select="ragione_sociale,non_conformita,delta_giorni")
    df = pd.DataFrame(rows)
    if df.empty: return []
    grp = df.groupby("ragione_sociale").agg(
        n_totale=("non_conformita", "count"),
        n_nc=("non_conformita", "sum"),
        avg_delta=("delta_giorni", "mean")
    ).reset_index()
    grp["perc_nc"] = (grp["n_nc"] / grp["n_totale"] * 100).round(2)
    return grp[grp["n_nc"] > 0].nlargest(limit, "n_nc").to_dict(orient="records")

@router.get("/nc/per-tipo")
def api_nc_tipo():
    rows = query(sb(), "non_conformita", select="tipo_origine,non_conformita,delta_giorni")
    df = pd.DataFrame(rows)
    if df.empty: return []
    return [{"tipo": t, "n_totale": len(g), "n_nc": int(g["non_conformita"].sum()),
             "perc_nc": safe_pct(int(g["non_conformita"].sum()), len(g)),
             "avg_delta": round(float(g["delta_giorni"].mean()), 1)}
            for t, g in df.groupby("tipo_origine")]
