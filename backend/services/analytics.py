"""
services/analytics.py — Analytics service layer
Tutte le query analytics passano da qui. Isolation totale dal layer HTTP.
Le analytics non sanno nulla di HTTP, file, o upload.
"""
import logging
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from functools import lru_cache

log = logging.getLogger("ua.analytics")

# ── Database query ────────────────────────────────────────────────

PAGE_SIZE = 1000

def query(client, table: str, filters=None, select: str = "*") -> list:
    """Query Supabase con paginazione automatica."""
    all_rows, offset = [], 0
    while True:
        q = client.table(table).select(select)
        if filters:
            for fn_filter in filters:
                q = fn_filter(q)
        batch = q.range(offset, offset + PAGE_SIZE - 1).execute().data
        all_rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return all_rows


def saving_filters(
    anno: Optional[int] = None,
    str_ric: Optional[str] = None,
    cdc: Optional[str] = None,
    alfa: Optional[str] = None,
    macro: Optional[str] = None,
    pref_comm: Optional[str] = None,
) -> list:
    """Costruisce lista di filtri Supabase per tabella saving."""
    fs = []
    if anno:
        fs.append(lambda q, a=anno:
            q.gte("data_doc", f"{a}-01-01").lte("data_doc", f"{a}-12-31"))
    if str_ric:   fs.append(lambda q, v=str_ric: q.eq("str_ric", v))
    if cdc:       fs.append(lambda q, v=cdc: q.eq("cdc", v))
    if alfa:      fs.append(lambda q, v=alfa: q.eq("alfa_documento", v))
    if macro:     fs.append(lambda q, v=macro.strip(): q.ilike("macro_categoria", f"%{v}%"))
    if pref_comm: fs.append(lambda q, v=pref_comm: q.eq("prefisso_commessa", v))
    return fs


def get_saving_df(
    client,
    anno: Optional[int] = None,
    str_ric: Optional[str] = None,
    cdc: Optional[str] = None,
    alfa: Optional[str] = None,
    macro: Optional[str] = None,
    pref_comm: Optional[str] = None,
    cols: str = "*",
) -> pd.DataFrame:
    """Carica saving dal DB normalizzato in DataFrame."""
    rows = query(
        client,
        "saving",
        saving_filters(anno, str_ric, cdc, alfa, macro, pref_comm),
        cols
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df['data_doc'] = pd.to_datetime(df.get('data_doc', pd.Series()), errors='coerce')
    for c in ['imp_listino_eur', 'imp_impegnato_eur', 'saving_eur']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    for c in ['negoziazione', 'accred_albo']:
        if c in df.columns:
            df[c] = df[c].fillna(False).astype(bool)

    return df


# ── KPI Service ───────────────────────────────────────────────────

from domain import calc_kpi, safe_pct

MESI = {1:"Gen",2:"Feb",3:"Mar",4:"Apr",5:"Mag",6:"Giu",
        7:"Lug",8:"Ago",9:"Set",10:"Ott",11:"Nov",12:"Dic"}

GRAN_MAP = {
    "mensile":    [(m, m, f"M{m:02d}") for m in range(1, 13)],
    "bimestrale": [(1,2,"B1"),(3,4,"B2"),(5,6,"B3"),(7,8,"B4"),(9,10,"B5"),(11,12,"B6")],
    "quarter":    [(1,3,"Q1"),(4,6,"Q2"),(7,9,"Q3"),(10,12,"Q4")],
    "semestrale": [(1,6,"S1"),(7,12,"S2")],
    "annuale":    [(1,12,"Anno")],
}


def get_anni(client) -> List[Dict]:
    rows = query(client, "saving", select="data_doc")
    df = pd.DataFrame(rows)
    if df.empty: return []
    anni = sorted(
        pd.to_datetime(df["data_doc"]).dt.year.dropna().unique().astype(int).tolist(),
        reverse=True
    )
    return [{"anno": a} for a in anni]


def kpi_riepilogo(client, anno=None, str_ric=None, cdc=None, alfa=None, macro=None) -> dict:
    df = get_saving_df(client, anno, str_ric, cdc, alfa, macro,
        cols="imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    return calc_kpi(df)


def kpi_mensile(client, anno=None, str_ric=None, cdc=None) -> list:
    df = get_saving_df(client, anno, str_ric, cdc,
        cols="data_doc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo")
    if df.empty: return []
    df["mese"] = df["data_doc"].dt.strftime("%Y-%m")
    return sorted([{"mese": m, **calc_kpi(g)} for m, g in df.groupby("mese")],
                  key=lambda x: x["mese"])


def kpi_mensile_area(client, anno=None, cdc=None) -> list:
    df = get_saving_df(client, anno, cdc=cdc,
        cols="data_doc,str_ric,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo")
    if df.empty: return []
    df["mese"] = df["data_doc"].dt.strftime("%Y-%m")
    MLBL = {f"{anno}-{m:02d}": MESI[m] for m in range(1, 13)} if anno else {}
    result = []
    for mese, grp in df.groupby("mese"):
        result.append({
            "mese": mese, "label": MLBL.get(mese, mese),
            **{f"tot_{k}": v for k, v in calc_kpi(grp).items()},
            **{f"ric_{k}": v for k, v in calc_kpi(grp[grp["str_ric"] == "RICERCA"]).items()},
            **{f"str_{k}": v for k, v in calc_kpi(grp[grp["str_ric"] == "STRUTTURA"]).items()},
        })
    return sorted(result, key=lambda x: x["mese"])


def kpi_per_cdc(client, anno=None, str_ric=None) -> list:
    df = get_saving_df(client, anno, str_ric,
        cols="cdc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    return sorted([{"cdc": c, **calc_kpi(g)} for c, g in df.groupby("cdc") if c],
                  key=lambda x: x["saving"], reverse=True)


def kpi_per_buyer(client, anno=None, str_ric=None, cdc=None) -> list:
    df = get_saving_df(client, anno, str_ric, cdc,
        cols="utente_presentazione,utente,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df["buyer"] = df["utente_presentazione"].fillna(df["utente"])
    return sorted(
        [{"utente": b, **calc_kpi(g)} for b, g in df.groupby("buyer")
         if b and str(b).strip() not in ('nan', 'none', '')],
        key=lambda x: x["saving"], reverse=True
    )


def kpi_per_alfa(client, anno=None, str_ric=None, cdc=None) -> list:
    df = get_saving_df(client, anno, str_ric, cdc,
        cols="alfa_documento,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo")
    if df.empty: return []
    return sorted(
        [{"alfa_documento": a, **calc_kpi(g)} for a, g in df.groupby("alfa_documento") if a],
        key=lambda x: x["listino"], reverse=True
    )


def kpi_per_macro(client, anno=None, str_ric=None, cdc=None) -> list:
    df = get_saving_df(client, anno, str_ric, cdc,
        cols="macro_categoria,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df["macro_categoria"] = df["macro_categoria"].fillna("Non classificato").str.strip()
    return sorted(
        [{"macro_categoria": m, **calc_kpi(g)} for m, g in df.groupby("macro_categoria")],
        key=lambda x: x["saving"], reverse=True
    )


def kpi_per_commessa(client, anno=None, cdc=None, limit=20) -> list:
    df = get_saving_df(client, anno, "RICERCA", cdc,
        cols="prefisso_commessa,desc_commessa,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df = df.dropna(subset=["prefisso_commessa"])
    result = []
    for pref, g in df.groupby("prefisso_commessa"):
        k = calc_kpi(g)
        desc = g["desc_commessa"].dropna().mode()
        result.append({"prefisso_commessa": pref, "desc_commessa": desc.iloc[0] if not desc.empty else "—", **k})
    return sorted(result, key=lambda x: x["saving"], reverse=True)[:limit]


def kpi_top_fornitori(client, anno=None, per="saving", limit=10, str_ric=None, cdc=None) -> list:
    df = get_saving_df(client, anno, str_ric, cdc,
        cols="ragione_sociale,accred_albo,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento")
    if df.empty: return []
    result = []
    for forn, g in df.groupby("ragione_sociale"):
        if not forn: continue
        k = calc_kpi(g)
        k["ragione_sociale"] = forn
        k["albo"] = bool(g["accred_albo"].mode().iloc[0]) if not g.empty else False
        result.append(k)
    sort_key = per if per in ("saving", "listino", "impegnato") else "saving"
    return sorted(result, key=lambda x: x.get(sort_key, 0), reverse=True)[:limit]


def kpi_pareto(client, anno=None, str_ric=None) -> list:
    df = get_saving_df(client, anno, str_ric, cols="ragione_sociale,imp_impegnato_eur")
    if df.empty: return []
    grp = (df.groupby("ragione_sociale")["imp_impegnato_eur"].sum()
            .sort_values(ascending=False).reset_index())
    total = grp["imp_impegnato_eur"].sum()
    grp["cum_perc"] = (grp["imp_impegnato_eur"].cumsum() / total * 100).round(2)
    grp["rank"] = range(1, len(grp) + 1)
    return grp.to_dict(orient="records")


def kpi_concentration(client, anno=None, str_ric=None) -> dict:
    df = get_saving_df(client, anno, str_ric, cols="ragione_sociale,imp_impegnato_eur")
    if df.empty: return {}
    total = float(df["imp_impegnato_eur"].fillna(0).sum())
    if total == 0: return {}
    grp = (df.groupby("ragione_sociale")["imp_impegnato_eur"].sum()
            .sort_values(ascending=False).reset_index())
    grp["share"] = (grp["imp_impegnato_eur"] / total * 100).round(2)
    n = len(grp)
    def cumshare(k): return round(float(grp.head(k)["share"].sum()), 2) if k <= n else 100.0
    hhi = round(float((grp["share"] ** 2).sum()), 1)
    return {
        "n_fornitori_totali": n, "total_impegnato": round(total, 2),
        "share_top_5":  cumshare(5), "share_top_10": cumshare(10), "share_top_20": cumshare(20),
        "hhi": hhi,
        "hhi_interpretation": (
            "Mercato molto concentrato" if hhi > 2500 else
            "Mercato concentrato" if hhi > 1500 else
            "Mercato moderatamente concentrato" if hhi > 1000 else
            "Mercato non concentrato"
        ),
        "top_5": grp.head(5)[["ragione_sociale","imp_impegnato_eur","share"]].to_dict(orient="records"),
    }


def kpi_executive_summary(client, anno=None, str_ric=None, cdc=None) -> dict:
    """Sintesi executive con KPI economici + rischio concentrazione fornitori."""
    kpi = kpi_riepilogo(client, anno, str_ric, cdc)
    conc = kpi_concentration(client, anno, str_ric)
    listino = float(kpi.get("listino", 0) or 0)
    impegnato = float(kpi.get("impegnato", 0) or 0)
    saving = float(kpi.get("saving", 0) or 0)
    return {
        **kpi,
        "saving_on_listino_pct": round((saving / listino) * 100, 2) if listino else None,
        "impegnato_on_listino_pct": round((impegnato / listino) * 100, 2) if listino else None,
        "supplier_hhi": conc.get("hhi"),
        "supplier_top5_share_pct": conc.get("share_top_5"),
        "supplier_top10_share_pct": conc.get("share_top_10"),
        "supplier_concentration_note": conc.get("hhi_interpretation"),
    }


def kpi_valute(client, anno=None) -> list:
    """Spesa in EUR per valuta originale (tutti i valori convertiti in EUR)."""
    df = get_saving_df(client, anno, cols="valuta,imp_listino_eur,imp_impegnato_eur")
    if df.empty: return []
    grp = df.groupby("valuta").agg(
        listino_eur=("imp_listino_eur", "sum"),
        impegnato_eur=("imp_impegnato_eur", "sum"),
        n_ordini=("imp_impegnato_eur", "count")
    ).reset_index()
    total = grp["impegnato_eur"].sum()
    grp["perc"] = (grp["impegnato_eur"] / total * 100).round(2)
    return grp.sort_values("impegnato_eur", ascending=False).to_dict(orient="records")


def kpi_valute_esposizione(client, anno=None) -> dict:
    """
    Esposizione valutaria in valuta ORIGINALE (non convertita).
    Fornisce la visibilità sul rischio cambio effettivo.
    Include: controvalore EUR calcolato + importo originale + cambio medio usato.
    """
    df = get_saving_df(
        client, anno,
        cols="valuta,cambio,imp_iniziale,imp_negoziato,imp_listino_eur,imp_impegnato_eur,saving_eur,data_doc"
    )
    if df.empty:
        return {"totale_eur": 0, "n_valute": 0, "valute": []}

    for c in ["imp_iniziale", "imp_negoziato", "cambio"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    result = []
    total_eur = float(df["imp_impegnato_eur"].sum()) if "imp_impegnato_eur" in df.columns else 0

    for valuta, g in df.groupby("valuta"):
        n = len(g)
        importo_orig = float(g["imp_iniziale"].sum()) if "imp_iniziale" in g.columns else 0
        negoziato_orig = float(g["imp_negoziato"].sum()) if "imp_negoziato" in g.columns else 0
        impegnato_eur = float(g["imp_impegnato_eur"].sum()) if "imp_impegnato_eur" in g.columns else 0
        listino_eur = float(g["imp_listino_eur"].sum()) if "imp_listino_eur" in g.columns else 0
        saving_eur_v = float(g["saving_eur"].sum()) if "saving_eur" in g.columns else 0

        # Cambio medio ponderato (esclude zero)
        cambi_validi = g["cambio"][g["cambio"] > 0] if "cambio" in g.columns else pd.Series([])
        cambio_medio = float(cambi_validi.mean()) if len(cambi_validi) > 0 else 1.0

        perc_su_totale = round(impegnato_eur / total_eur * 100, 2) if total_eur > 0 else 0

        result.append({
            "valuta": valuta,
            "n_ordini": n,
            "importo_originale": round(importo_orig, 2),
            "negoziato_originale": round(negoziato_orig, 2),
            "impegnato_eur": round(impegnato_eur, 2),
            "listino_eur": round(listino_eur, 2),
            "saving_eur": round(saving_eur_v, 2),
            "cambio_medio": round(cambio_medio, 6),
            "perc_su_totale_eur": perc_su_totale,
            "is_foreign": valuta.upper().strip() not in {"EUR", "EURO", "€"},
        })

    result.sort(key=lambda x: x["impegnato_eur"], reverse=True)
    foreign = [r for r in result if r["is_foreign"]]

    return {
        "totale_eur": round(total_eur, 2),
        "n_valute": len(result),
        "n_valute_estere": len(foreign),
        "esposizione_estera_eur": round(sum(r["impegnato_eur"] for r in foreign), 2),
        "perc_esposizione_estera": round(
            sum(r["impegnato_eur"] for r in foreign) / total_eur * 100, 2
        ) if total_eur > 0 else 0,
        "valute": result,
    }


def kpi_yoy(
    client, anno: int, granularita: str = "mensile",
    str_ric=None, cdc=None
) -> dict:
    """YoY granulare — confronto anno corrente vs anno precedente."""
    ap = anno - 1
    periodi = GRAN_MAP.get(granularita, GRAN_MAP["mensile"])
    cols = "data_doc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo"

    df_c = get_saving_df(client, anno, str_ric, cdc, cols=cols)
    df_p = get_saving_df(client, ap,   str_ric, cdc, cols=cols)

    if not df_c.empty:
        df_c["mn"] = df_c["data_doc"].dt.month
        df_c = df_c.dropna(subset=["mn"])
        if not df_c.empty:
            df_c["mn"] = df_c["mn"].astype(int)
    if not df_p.empty:
        df_p["mn"] = df_p["data_doc"].dt.month
        df_p = df_p.dropna(subset=["mn"])
        if not df_p.empty:
            df_p["mn"] = df_p["mn"].astype(int)

    mese_max = int(df_c["mn"].max()) if not df_c.empty else 0
    ult_giorno = int(df_c[df_c["mn"] == mese_max]["data_doc"].dt.day.max()) if mese_max and not df_c.empty else 0

    def delta(c, p): return round((c - p) / abs(p) * 100, 1) if p else None

    chart = []
    for m1, m2, lbl in periodi:
        gc = df_c[(df_c["mn"] >= m1) & (df_c["mn"] <= m2)] if not df_c.empty else pd.DataFrame()
        gp = df_p[(df_p["mn"] >= m1) & (df_p["mn"] <= m2)] if not df_p.empty else pd.DataFrame()
        if len(gc) == 0 and len(gp) == 0: continue

        parziale = len(gc) > 0 and mese_max < m2
        if granularita == "mensile": label = MESI.get(m1, lbl)
        elif granularita == "quarter": label = lbl
        else: label = f"{MESI.get(m1, lbl)}–{MESI.get(m2, lbl)}"

        kc, kp = calc_kpi(gc), calc_kpi(gp)
        chart.append({
            "label": label, "m_start": m1, "m_end": m2, "parziale": parziale,
            "ha_dati_curr": len(gc) > 0, "ha_dati_prev": len(gp) > 0,
            f"listino_{anno}": kc["listino"], f"impegnato_{anno}": kc["impegnato"],
            f"saving_{anno}": kc["saving"], f"perc_saving_{anno}": kc["perc_saving"],
            f"listino_{ap}": kp["listino"], f"impegnato_{ap}": kp["impegnato"],
            f"saving_{ap}": kp["saving"], f"perc_saving_{ap}": kp["perc_saving"],
            "delta_saving": delta(kc["saving"], kp["saving"]) if not parziale else None,
            "delta_impegnato": delta(kc["impegnato"], kp["impegnato"]) if not parziale else None,
        })

    mesi_interi = {m for r in chart for m in range(r["m_start"], r["m_end"]+1)
                   if not r["parziale"] and r["ha_dati_curr"] and r["ha_dati_prev"]}
    kc_hl = calc_kpi(df_c[df_c["mn"].isin(mesi_interi)] if not df_c.empty and mesi_interi else df_c)
    kp_hl = calc_kpi(df_p[df_p["mn"].isin(mesi_interi)] if not df_p.empty and mesi_interi else df_p)
    mc = max(mesi_interi) if mesi_interi else mese_max

    nota = ""
    if mese_max and mese_max < 12:
        nota = f"Dati {anno} disponibili fino al {df_c['data_doc'].max().date() if not df_c.empty else '—'}."
        if ult_giorno < 20 and mese_max > 1:
            nota += f" {MESI.get(mese_max, '')} è parziale ed escluso dal confronto."

    return {
        "anno": anno, "anno_precedente": ap, "granularita": granularita,
        "chart_data": chart,
        "kpi_headline": {
            "corrente": kc_hl, "precedente": kp_hl,
            "label_curr": f"Gen–{MESI.get(mc,'?')} {anno}",
            "label_prev": f"Gen–{MESI.get(mc,'?')} {ap}",
            "delta": {
                "listino":    delta(kc_hl["listino"],    kp_hl["listino"]),
                "impegnato":  delta(kc_hl["impegnato"],  kp_hl["impegnato"]),
                "saving":     delta(kc_hl["saving"],     kp_hl["saving"]),
                "perc_saving": round(kc_hl["perc_saving"] - kp_hl["perc_saving"], 2)
                               if kp_hl["perc_saving"] else None,
            }
        },
        "nota": nota, "mese_max": mese_max, "ultimo_giorno": ult_giorno,
    }


def kpi_yoy_cdc(client, anno: int) -> list:
    ap = anno - 1
    cols = "cdc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo"
    df_c = get_saving_df(client, anno,  cols=cols)
    df_p = get_saving_df(client, ap,    cols=cols)
    def by_cdc(df): return {c: calc_kpi(g) for c, g in df.groupby("cdc") if c} if not df.empty else {}
    curr, prev = by_cdc(df_c), by_cdc(df_p)
    all_cdc = sorted(set(list(curr) + list(prev)))
    return [{
        "cdc": c,
        f"saving_{anno}":    curr.get(c, {}).get("saving", 0),
        f"saving_{ap}":      prev.get(c, {}).get("saving", 0),
        f"impegnato_{anno}": curr.get(c, {}).get("impegnato", 0),
        f"impegnato_{ap}":   prev.get(c, {}).get("impegnato", 0),
    } for c in all_cdc]


def kpi_per_protocollo_commessa(client, anno=None, str_ric=None, cdc=None, limit=20) -> list:
    df = get_saving_df(client, anno, str_ric, cdc,
        cols="protoc_commessa,prefisso_commessa,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo,ragione_sociale")
    if df.empty: return []
    df = df.dropna(subset=["protoc_commessa"])
    result = []
    for prot, g in df.groupby("protoc_commessa"):
        k = calc_kpi(g)
        result.append({"protocollo": prot, "n_fornitori": g["ragione_sociale"].nunique(), **k})
    return sorted(result, key=lambda x: x["impegnato"], reverse=True)[:limit]


def kpi_per_protocollo_ordine(client, anno=None, str_ric=None, limit=20) -> list:
    df = get_saving_df(client, anno, str_ric,
        cols="protoc_ordine,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo")
    if df.empty: return []
    df = df.dropna(subset=["protoc_ordine"])
    df["protoc_ordine"] = df["protoc_ordine"].astype(str)
    result = [{"protocollo_ordine": p, **calc_kpi(g)} for p, g in df.groupby("protoc_ordine")]
    return sorted(result, key=lambda x: x["impegnato"], reverse=True)[:limit]


def kpi_per_buyer_cdc(client, anno=None) -> list:
    df = get_saving_df(client, anno,
        cols="utente_presentazione,utente,cdc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df["buyer"] = df["utente_presentazione"].fillna(df["utente"]).fillna("N/D")
    result = []
    for (buyer, cdc_v), g in df.groupby(["buyer", "cdc"]):
        if not buyer or not cdc_v: continue
        result.append({"buyer": buyer, "cdc": cdc_v, **calc_kpi(g)})
    return sorted(result, key=lambda x: x["saving"], reverse=True)


# ══════════════════════════════════════════════════════════════════
# AUTO-INSIGHTS ENGINE — genera insight testuali dai dati
# ══════════════════════════════════════════════════════════════════

def _fmt_eur(v: float) -> str:
    """Formatta un valore in EUR in modo leggibile."""
    if abs(v) >= 1_000_000:
        return f"€{v/1_000_000:.1f}M"
    if abs(v) >= 1_000:
        return f"€{v/1_000:.0f}K"
    return f"€{v:.0f}"

def _delta_str(delta: Optional[float], unit: str = "%") -> str:
    if delta is None: return ""
    sign = "+" if delta > 0 else ""
    return f"{sign}{delta:.1f}{unit}"


def kpi_insights(client, anno: Optional[int] = None, str_ric: Optional[str] = None) -> List[Dict]:
    """
    Genera automaticamente un elenco di insight testuali dai dati.
    Ogni insight ha:
      - type: "positive" | "warning" | "info" | "alert"
      - category: "saving" | "fornitori" | "efficienza" | "budget" | "trend"
      - title: stringa breve
      - body: descrizione completa
      - metric: valore principale (opzionale)
      - delta: variazione YoY (opzionale)
      - priority: 1-5 (1 = più importante)
    """
    insights: List[Dict] = []
    anni_data = get_anni(client)
    anni_list = [a["anno"] for a in anni_data]

    if not anni_list:
        return []

    # Anno corrente e precedente
    anno_curr = anno or max(anni_list)
    anno_prev = anno_curr - 1
    has_prev = anno_prev in anni_list

    # ── Dati saving ───────────────────────────────────────────────
    cols_full = "imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento,cdc,ragione_sociale,data_doc,macro_categoria,utente_presentazione,utente"
    df_curr = get_saving_df(client, anno_curr, str_ric, cols=cols_full)
    df_prev = get_saving_df(client, anno_prev, str_ric, cols=cols_full) if has_prev else pd.DataFrame()

    kc = calc_kpi(df_curr)
    kp = calc_kpi(df_prev) if not df_prev.empty else None

    # ── INSIGHT 1: Saving rate ────────────────────────────────────
    perc_saving = kc.get("perc_saving", 0)
    if perc_saving > 0:
        t = "positive" if perc_saving >= 8 else ("warning" if perc_saving < 5 else "info")
        delta_ps = None
        if kp:
            delta_ps = round(perc_saving - kp.get("perc_saving", 0), 1)
        insights.append({
            "type": t,
            "category": "saving",
            "title": f"Saving rate {anno_curr}: {perc_saving:.1f}%",
            "body": (
                f"Il saving rate complessivo {anno_curr} è del {perc_saving:.1f}% "
                f"({_fmt_eur(kc.get('saving', 0))} su {_fmt_eur(kc.get('listino', 0))} di listino)."
                + (f" Variazione YoY: {_delta_str(delta_ps, 'pp')}." if delta_ps is not None else "")
                + (" Risultato eccellente — sopra la soglia target del 8%." if perc_saving >= 8 else
                   " Risultato sotto la soglia target del 5% — priorità negoziale richiesta." if perc_saving < 5 else
                   " Risultato nella norma — margini di miglioramento disponibili.")
            ),
            "metric": f"{perc_saving:.1f}%",
            "delta": _delta_str(delta_ps, " pp") if delta_ps is not None else None,
            "priority": 1,
        })

    # ── INSIGHT 2: Tasso negoziazione ────────────────────────────
    perc_neg = kc.get("perc_negoziati", 0)
    n_neg = kc.get("n_negoziati", 0)
    n_neg_doc = kc.get("n_doc_neg", 0) or kc.get("n_negotiable", 0) or n_neg
    if n_neg_doc > 0:
        delta_neg = None
        if kp:
            delta_neg = round(perc_neg - kp.get("perc_negoziati", 0), 1)
        t = "positive" if perc_neg >= 70 else ("warning" if perc_neg < 40 else "info")
        insights.append({
            "type": t,
            "category": "efficienza",
            "title": f"Negoziazione: {perc_neg:.0f}% degli ordini trattati",
            "body": (
                f"Su {n_neg_doc:,} ordini negoziabili, {n_neg:,} ({perc_neg:.0f}%) "
                f"hanno beneficiato di una negoziazione attiva."
                + (f" YoY: {_delta_str(delta_neg, 'pp')}." if delta_neg is not None else "")
                + (" Ottima copertura negoziale." if perc_neg >= 70 else
                   " Attenzione: bassa copertura negoziale — opportunità di saving non catturate." if perc_neg < 40 else "")
            ),
            "metric": f"{perc_neg:.0f}%",
            "delta": _delta_str(delta_neg, " pp") if delta_neg is not None else None,
            "priority": 2,
        })

    # ── INSIGHT 3: Concentrazione fornitori ──────────────────────
    if not df_curr.empty and "ragione_sociale" in df_curr.columns:
        grp_forn = (
            df_curr.groupby("ragione_sociale")["imp_impegnato_eur"]
            .sum().sort_values(ascending=False)
        )
        total_spend = float(grp_forn.sum())
        if total_spend > 0 and len(grp_forn) > 0:
            top5_share = float(grp_forn.head(5).sum()) / total_spend * 100
            n_forn = len(grp_forn)
            top1 = grp_forn.index[0] if len(grp_forn) > 0 else "N/D"
            top1_share = float(grp_forn.iloc[0]) / total_spend * 100
            t = "warning" if top5_share > 60 or top1_share > 25 else "info"
            insights.append({
                "type": t,
                "category": "fornitori",
                "title": f"Concentrazione fornitori: top 5 = {top5_share:.0f}% della spesa",
                "body": (
                    f"Su {n_forn} fornitori attivi, i primi 5 rappresentano il {top5_share:.0f}% "
                    f"della spesa totale ({_fmt_eur(total_spend)})."
                    f" Il fornitore principale è {top1} ({top1_share:.0f}% del totale)."
                    + (" Rischio concentrazione elevato: dipendenza da pochi fornitori." if top5_share > 60 else
                       " Portafoglio fornitori diversificato.")
                ),
                "metric": f"{top5_share:.0f}%",
                "delta": None,
                "priority": 3,
            })

    # ── INSIGHT 4: Andamento YoY saving ──────────────────────────
    if kp and kc.get("saving", 0) > 0 and kp.get("saving", 0) > 0:
        delta_sav = round((kc["saving"] - kp["saving"]) / abs(kp["saving"]) * 100, 1)
        delta_lst = round((kc["listino"] - kp["listino"]) / abs(kp["listino"]) * 100, 1) if kp["listino"] else None
        t = "positive" if delta_sav > 5 else ("warning" if delta_sav < -5 else "info")
        insights.append({
            "type": t,
            "category": "trend",
            "title": f"Saving YoY {anno_prev}→{anno_curr}: {_delta_str(delta_sav)}",
            "body": (
                f"Il saving {anno_curr} ({_fmt_eur(kc['saving'])}) "
                f"{'supera' if delta_sav > 0 else 'è inferiore a'} "
                f"quello {anno_prev} ({_fmt_eur(kp['saving'])}) del {abs(delta_sav):.1f}%."
                + (f" Il volume di spesa (listino) è variato del {_delta_str(delta_lst)}." if delta_lst else "")
            ),
            "metric": _fmt_eur(kc["saving"]),
            "delta": _delta_str(delta_sav),
            "priority": 2,
        })

    # ── INSIGHT 5: CDC con saving più alto ───────────────────────
    if not df_curr.empty and "cdc" in df_curr.columns:
        cdc_grp = df_curr.groupby("cdc").apply(calc_kpi).reset_index()
        if not cdc_grp.empty:
            # cdc_grp è una Series di dizionari — convertiamo
            cdc_rows = []
            for cdc_v, g in df_curr.groupby("cdc"):
                if cdc_v:
                    k = calc_kpi(g)
                    if k["listino"] > 0:
                        cdc_rows.append({"cdc": cdc_v, **k})
            if cdc_rows:
                best_cdc = max(cdc_rows, key=lambda x: x["perc_saving"])
                worst_cdc = min(cdc_rows, key=lambda x: x["perc_saving"])
                insights.append({
                    "type": "info",
                    "category": "saving",
                    "title": f"Miglior CDC: {best_cdc['cdc']} ({best_cdc['perc_saving']:.1f}% saving)",
                    "body": (
                        f"Il centro di costo con saving rate più alto è {best_cdc['cdc']} "
                        f"({best_cdc['perc_saving']:.1f}%, {_fmt_eur(best_cdc['saving'])})."
                        f" Il CDC con saving rate più basso è {worst_cdc['cdc']} "
                        f"({worst_cdc['perc_saving']:.1f}%). "
                        f"Gap di {best_cdc['perc_saving'] - worst_cdc['perc_saving']:.1f} punti percentuali."
                    ),
                    "metric": f"{best_cdc['perc_saving']:.1f}%",
                    "delta": None,
                    "priority": 4,
                })

    # ── INSIGHT 6: Fornitore albo ────────────────────────────────
    perc_albo = kc.get("perc_albo", 0)
    if perc_albo > 0:
        t = "positive" if perc_albo >= 60 else ("warning" if perc_albo < 30 else "info")
        insights.append({
            "type": t,
            "category": "fornitori",
            "title": f"Fornitori accreditati: {perc_albo:.0f}% degli ordini",
            "body": (
                f"Il {perc_albo:.0f}% degli ordini {anno_curr} è stato assegnato a fornitori accreditati all'Albo."
                + (" Ottimo utilizzo dell'albo fornitori qualificato." if perc_albo >= 60 else
                   " Attenzione: basso utilizzo dei fornitori accreditati — rischio qualità e compliance." if perc_albo < 30 else "")
            ),
            "metric": f"{perc_albo:.0f}%",
            "delta": None,
            "priority": 4,
        })

    # ── INSIGHT 7: Macro categoria con più saving ────────────────
    if not df_curr.empty and "macro_categoria" in df_curr.columns:
        mc_rows = []
        for mc, g in df_curr.groupby("macro_categoria"):
            if mc and str(mc).strip() not in ("", "nan", "None"):
                k = calc_kpi(g)
                if k["saving"] > 0:
                    mc_rows.append({"macro": str(mc).strip(), **k})
        if mc_rows:
            top_mc = sorted(mc_rows, key=lambda x: x["saving"], reverse=True)[:3]
            names = ", ".join(f"{r['macro']} ({_fmt_eur(r['saving'])})" for r in top_mc)
            insights.append({
                "type": "info",
                "category": "budget",
                "title": f"Top categoria per saving: {top_mc[0]['macro']}",
                "body": (
                    f"Le macro categorie con maggior saving {anno_curr} sono: {names}."
                    f" La categoria {top_mc[0]['macro']} contribuisce il "
                    f"{top_mc[0]['perc_saving']:.1f}% di saving rate."
                ),
                "metric": _fmt_eur(top_mc[0]["saving"]),
                "delta": None,
                "priority": 5,
            })

    # Ordina per priorità e poi per tipo (positive/alert prima)
    TYPE_ORDER = {"alert": 0, "warning": 1, "positive": 2, "info": 3}
    insights.sort(key=lambda x: (x["priority"], TYPE_ORDER.get(x["type"], 9)))

    return insights
