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

    if not df_c.empty: df_c["mn"] = df_c["data_doc"].dt.month
    if not df_p.empty: df_p["mn"] = df_p["data_doc"].dt.month

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
