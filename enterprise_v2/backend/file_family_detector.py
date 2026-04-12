from typing import Iterable


def detect_file_family(column_names: Iterable[str]) -> str:
    normalized = {str(c).strip().lower() for c in column_names if c}

    # Saving / Orders
    if (
        {"saving", "impegnato", "fornitore"} & normalized
        or {"imp iniziale", "imp negoziato", "alfa documento"} <= normalized
        or {"ragione sociale fornitore", "data doc", "protoc commessa"} <= normalized
    ):
        return "saving_orders"

    # Detailed orders / line items
    if (
        {"cod articolo", "descrizione articolo"} <= normalized
        or {"qta 1 doc", "prezzo netto 1", "importo riga"} & normalized
        or {"cod documento", "nr doc", "ragione sociale anagrafica"} <= normalized
    ):
        return "detailed_orders"

    # Resources / team
    if (
        {"risorsa", "pratiche gestite", "saving generato"} & normalized
        or {"resource", "cases managed", "savings generated"} & normalized
    ):
        return "resources_team"

    # Cycle times
    if (
        {"days purchasing", "days auto", "total days", "bottleneck"} <= normalized
        or {"year month", "total days", "bottleneck"} <= normalized
    ):
        return "cycle_times"

    # Non conformities
    if (
        {"non conformità", "data origine", "ragione sociale anagrafica"} <= normalized
        or {"tipo origine", "delta giorni (fattura origine)", "non conformità"} & normalized
    ):
        return "non_conformities"

    # Suppliers master
    if {"albo", "supplier", "vendor"} & normalized:
        return "suppliers_master"

    return "unknown"
