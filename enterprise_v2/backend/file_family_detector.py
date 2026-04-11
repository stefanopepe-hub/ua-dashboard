from typing import Iterable


def detect_file_family(column_names: Iterable[str]) -> str:
    normalized = {str(c).strip().lower() for c in column_names if c}

    if {"saving", "impegnato", "fornitore"} & normalized:
        return "saving_orders"

    if {"articolo", "descrizione articolo", "quantità"} & normalized:
        return "detailed_orders"

    if {"risorsa", "pratiche gestite", "saving generato"} & normalized:
        return "resources_team"

    if {"tempo", "giorni", "fase acquisti"} & normalized:
        return "cycle_times"

    if {"non conformità", "gravità", "fornitore"} & normalized:
        return "non_conformities"

    if {"albo", "supplier", "vendor"} & normalized:
        return "suppliers_master"

    return "unknown"
