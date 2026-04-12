@app.get("/kpi/saving/per-commessa")
def kpi_per_commessa(
    anno: Optional[int] = Query(None),
    cdc: Optional[str] = Query(None),
    limit: int = Query(20),
):
    df = get_saving_df(
        anno,
        "RICERCA",
        cdc,
        cols="prefisso_commessa,desc_commessa,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento",
    )
    if df.empty:
        return []

    df = df.dropna(subset=["prefisso_commessa"])
    result = []

    for pref, g in df.groupby("prefisso_commessa"):
        k = calc_kpi(g)
        desc = g["desc_commessa"].dropna().mode()
        result.append({
            "prefisso_commessa": pref,
            "desc_commessa": desc.iloc[0] if not desc.empty else "—",
            **k,
        })

    return sorted(result, key=lambda x: x["saving"], reverse=True)[:limit]
