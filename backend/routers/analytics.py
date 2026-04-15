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
    kpi_concentration, kpi_executive_summary, kpi_valute, kpi_yoy, kpi_yoy_cdc,
    kpi_per_protocollo_commessa, kpi_per_protocollo_ordine, kpi_per_buyer_cdc,
    kpi_insights,
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

@router.get("/saving/executive-summary")
def api_executive_summary(
    anno: Optional[int] = Query(None),
    str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None),
):
    return kpi_executive_summary(sb(), anno, str_ric, cdc)

@router.get("/saving/valute")
def api_valute(anno: Optional[int] = Query(None)):
    return kpi_valute(sb(), anno)

@router.get("/saving/insights")
def api_insights(
    anno: Optional[int] = Query(None),
    str_ric: Optional[str] = Query(None),
):
    """
    Auto-insights engine: genera insight testuali automatici dai dati.
    Ogni insight include type, category, title, body, metric, delta, priority.
    """
    try:
        return kpi_insights(sb(), anno, str_ric)
    except Exception as e:
        log.error(f"insights error: {e}", exc_info=True)
        return []


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

def _get_risorse_df(anno: Optional[int] = None) -> pd.DataFrame:
    # 1) prova tabella dedicata resource_performance
    rows = query(sb(), "resource_performance")
    df = pd.DataFrame(rows)

    if not df.empty:
        if anno is not None and "year" in df.columns:
            df = df[df["year"] == anno]
        return df

    # 2) fallback: deriva le analytics Risorse dai dati già presenti in saving
    rows = query(
        sb(),
        "saving",
        select="data_doc,utente_presentazione,utente,str_ric,saving_eur,negoziazione,protoc_commessa,protoc_ordine"
    )
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    if "data_doc" in df.columns:
        df["data_doc"] = pd.to_datetime(df["data_doc"], errors="coerce")

    df = df.dropna(subset=["data_doc"])

    if anno is not None:
        df = df[df["data_doc"].dt.year == anno]

    if df.empty:
        return df

       # ============================================================
    # NORMALIZZAZIONE TEAM ACQUISTI
    # ============================================================
    TEAM_CANONICO = {
        "stefano pepe": "Stefano Pepe",
        "francesco di clemente": "Francesco Di Clemente",
        "silvana ruotolo": "Silvana Ruotolo",
        "marina padricelli": "Marina Padricelli",
        "luisa veneruso": "Luisa Veneruso",
        "katuscia leonardi": "Katuscia Leonardi",
        "francesca perazzetti": "Francesca Perazzetti",
        "loredana scialanga": "Loredana Scialanga",
        "mariacarla di matteo": "Mariacarla Di Matteo",
    }

    RESPONSABILI = {
        "Stefano Pepe": "Stefano Pepe",
        "Francesco Di Clemente": "Francesco Di Clemente",
        "Silvana Ruotolo": "Stefano Pepe",
        "Marina Padricelli": "Stefano Pepe",
        "Luisa Veneruso": "Stefano Pepe",
        "Loredana Scialanga": "Stefano Pepe",
        "Mariacarla Di Matteo": "Stefano Pepe",
        "Katuscia Leonardi": "Francesco Di Clemente",
        "Francesca Perazzetti": "Francesco Di Clemente",
    }

    ESCLUSIONI = {
        "",
        "n/d",
        "ordini diretti",
        "ordini diretti ricerca",
        "ordini diretti struttura",
        "pconsales",
        "corefice",
    }

    def _norm_name(x):
        if pd.isna(x):
            return None
        s = str(x).strip()
        s = " ".join(s.split())
        return s

    def _norm_key(x):
        if x is None:
            return None
        return _norm_name(x).lower()

    if "utente_presentazione" in df.columns:
        df["risorsa_raw"] = df["utente_presentazione"].fillna(df["utente"] if "utente" in df.columns else None)
    else:
        df["risorsa_raw"] = df["utente"] if "utente" in df.columns else None

    df["risorsa_raw"] = df["risorsa_raw"].apply(_norm_name)
    df["risorsa_key"] = df["risorsa_raw"].apply(_norm_key)

    # Tieni solo il team acquisti definito
    df = df[df["risorsa_key"].isin(TEAM_CANONICO.keys())]

    if df.empty:
        return df

    df["risorsa"] = df["risorsa_key"].map(TEAM_CANONICO)

    # Responsabile associato
    df["responsabile"] = df["risorsa"].map(RESPONSABILI).fillna("N/D")

    # Escludi eventuali etichette spurie residue
    df = df[~df["risorsa_key"].isin(ESCLUSIONI)]

    if df.empty:
        return df

    df["struttura"] = df["str_ric"].fillna("N/D") if "str_ric" in df.columns else "N/D"
    df["year"] = df["data_doc"].dt.year
    df["mese"] = df["data_doc"].dt.strftime("%Y-%m")
    df["mese_label"] = df["mese"]
    df["saving_generato"] = pd.to_numeric(
        df["saving_eur"] if "saving_eur" in df.columns else 0,
        errors="coerce"
    ).fillna(0)

    if "negoziazione" in df.columns:
        df["negoziazione"] = df["negoziazione"].fillna(False).astype(bool)
    else:
        df["negoziazione"] = False

    if "protoc_commessa" in df.columns:
        df["pratica_ref"] = df["protoc_commessa"].fillna(
            df["protoc_ordine"] if "protoc_ordine" in df.columns else None
        )
    else:
        df["pratica_ref"] = df["protoc_ordine"] if "protoc_ordine" in df.columns else None

    df["pratica_ref"] = df["pratica_ref"].fillna("N/D")

    grouped = (
        df.groupby(
            ["responsabile", "risorsa", "struttura", "year", "mese", "mese_label"],
            dropna=False
        )
        .agg(
            pratiche_gestite=("pratica_ref", "count"),
            pratiche_aperte=("pratica_ref", "count"),
            pratiche_chiuse=("pratica_ref", "count"),
            saving_generato=("saving_generato", "sum"),
            negoziazioni_concluse=("negoziazione", "sum"),
        )
        .reset_index()
    )

    grouped["tempo_medio_giorni"] = None
    grouped["efficienza"] = None

    return grouped


@router.get("/risorse/riepilogo")
def api_risorse_riepilogo(anno: Optional[int] = Query(None)):
    df = _get_risorse_df(anno)
    if df.empty:
        return {"available": False, "reason": "Nessun dataset risorse disponibile."}

    avg_pratiche = df["pratiche_gestite"].dropna().mean() if "pratiche_gestite" in df.columns else 0
    tot_saving = df["saving_generato"].dropna().sum() if "saving_generato" in df.columns else 0

    return {
        "available": True,
        "n_record": len(df),
        "n_risorse": int(df["risorsa"].dropna().nunique()) if "risorsa" in df.columns else 0,
        "avg_pratiche_gestite": round(float(avg_pratiche), 1) if pd.notna(avg_pratiche) else 0,
        "tot_saving_generato": round(float(tot_saving), 2) if pd.notna(tot_saving) else 0,
    }


@router.get("/risorse/per-risorsa")
def api_risorse_per_risorsa(anno: Optional[int] = Query(None)):
    df = _get_risorse_df(anno)
    if df.empty:
        return []

    result = []
    for risorsa, g in df.groupby("risorsa"):
               result.append({
            "responsabile": g["responsabile"].dropna().mode().iloc[0] if "responsabile" in g.columns and not g["responsabile"].dropna().empty else None,
            "risorsa": risorsa,
            "struttura": g["struttura"].dropna().mode().iloc[0] if not g["struttura"].dropna().empty else None,
            "pratiche_gestite": int(g["pratiche_gestite"].sum()) if "pratiche_gestite" in g.columns else 0,
            "pratiche_aperte": int(g["pratiche_aperte"].sum()) if "pratiche_aperte" in g.columns else 0,
            "pratiche_chiuse": int(g["pratiche_chiuse"].sum()) if "pratiche_chiuse" in g.columns else 0,
            "saving_generato": round(float(g["saving_generato"].sum()), 2) if "saving_generato" in g.columns else 0,
            "negoziazioni_concluse": int(g["negoziazioni_concluse"].sum()) if "negoziazioni_concluse" in g.columns else 0,
            "tempo_medio_giorni": None,
            "efficienza": None,
        })

    return sorted(result, key=lambda x: x["saving_generato"], reverse=True)


@router.get("/risorse/mensile")
def api_risorse_mensile(anno: Optional[int] = Query(None)):
    df = _get_risorse_df(anno)
    if df.empty:
        return []

    result = []
    for mese, g in df.groupby("mese_label"):
        result.append({
            "mese": mese,
            "pratiche_totali": int(g["pratiche_gestite"].sum()) if "pratiche_gestite" in g.columns else 0,
            "saving_totale": round(float(g["saving_generato"].sum()), 2) if "saving_generato" in g.columns else 0,
            "n_risorse_attive": int(g["risorsa"].nunique()) if "risorsa" in g.columns else 0,
        })

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
