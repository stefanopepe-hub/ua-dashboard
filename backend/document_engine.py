"""
document_engine.py — Motore semantico documenti procurement
Fondazione Telethon ETS — UA Dashboard Enterprise

Classificazione deterministica e auditabile di tutti i tipi documento.
Fonte unica di verità per semantica procurement.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Set
import re


class DocDomain(str, Enum):
    RICERCA   = "RICERCA"
    STRUTTURA = "STRUTTURA"
    LOGISTICA = "LOGISTICA"
    ECCEZIONALE = "ECCEZIONALE"
    UNKNOWN   = "UNKNOWN"


@dataclass(frozen=True)
class DocType:
    code: str
    label: str
    domain: DocDomain
    is_order: bool          # conta come ordine procurement
    contributes_saving: bool
    contributes_workload: bool
    contributes_logistics: bool
    description: str = ""


# ── Catalogo documentale autoritativo ─────────────────────────────────────────
DOCUMENT_CATALOG: dict[str, DocType] = {

    # RICERCA
    "ORN": DocType("ORN", "Ordine Ricerca",               DocDomain.RICERCA,   True,  True,  True,  False, "Ordine standard area ricerca"),
    "ORD": DocType("ORD", "Ordine Diretto Ricerca",       DocDomain.RICERCA,   True,  True,  True,  False, "Ordine diretto < soglia CIG"),
    "OPR": DocType("OPR", "Ordine Previsionale Ricerca",  DocDomain.RICERCA,   True,  True,  True,  False, "Ordine a budget previsionale ricerca"),
    "PS":  DocType("PS",  "Procedura Straordinaria",       DocDomain.RICERCA,   True,  True,  True,  False, "Procedura negoziata eccezionale"),
    "ORN01": DocType("ORN01", "Ordine Ricerca (var.)",     DocDomain.RICERCA,   True,  True,  True,  False),
    "ORD01": DocType("ORD01", "Ordine Diretto Ricerca (var.)", DocDomain.RICERCA, True, True, True,  False),
    "OPR01": DocType("OPR01", "Ordine Prev. Ricerca (var.)", DocDomain.RICERCA, True,  True,  True,  False),
    "COR-ORD": DocType("COR-ORD", "Correzione Ordine Ricerca", DocDomain.RICERCA, False, False, False, False),
    "PS006":   DocType("PS006", "Procedura Straordinaria (var.)", DocDomain.RICERCA, True, True, True, False),

    # STRUTTURA
    "OS":    DocType("OS",    "Ordine Struttura",                DocDomain.STRUTTURA, True,  True,  True,  False, "Ordine standard area struttura"),
    "OSD":   DocType("OSD",   "Ordine Diretto Struttura",        DocDomain.STRUTTURA, True,  True,  True,  False),
    "OSP":   DocType("OSP",   "Ordine Previsionale Struttura",   DocDomain.STRUTTURA, True,  True,  True,  False),
    "OSDP01":DocType("OSDP01","Ordine Diretto Struttura (var.)", DocDomain.STRUTTURA, True,  True,  True,  False),
    "RA501": DocType("RA501", "Richiesta Acquisto Struttura",     DocDomain.STRUTTURA, False, False, True,  False),

    # LOGISTICA / DDT
    "DDT":   DocType("DDT",  "Documento Trasporto",   DocDomain.LOGISTICA, False, False, False, True, "Bolla di consegna"),
    "DTR":   DocType("DTR",  "Documento Trasporto",   DocDomain.LOGISTICA, False, False, False, True),
    "DTR01": DocType("DTR01","Documento Trasporto (var.)", DocDomain.LOGISTICA, False, False, False, True),
}

# Codes che contribuiscono al saving procurement
ORDER_CODES_FOR_SAVING: frozenset = frozenset(
    c for c, d in DOCUMENT_CATALOG.items() if d.contributes_saving
)

# Codes che sono ordini "negoziabili" (usati per perc_negoziati)
NEGOTIABLE_ORDER_CODES: frozenset = frozenset(
    {"OS", "OSP", "PS", "OPR", "ORN", "ORD", "ORN01", "ORD01", "OPR01", "OSDP01", "PS006"}
)

# Codes logistica — esclusi da analytics saving/ordini
LOGISTICS_CODES: frozenset = frozenset(
    c for c, d in DOCUMENT_CATALOG.items() if d.domain == DocDomain.LOGISTICA
)


def classify_doc(code: str) -> DocType:
    """Classifica un codice documento. Mai fallisce."""
    if not code:
        return DocType("", "Non classificato", DocDomain.UNKNOWN, False, False, False, False)
    code_upper = str(code).strip().upper()
    return DOCUMENT_CATALOG.get(code_upper, DocType(
        code_upper, f"Tipo {code_upper}", DocDomain.UNKNOWN, True, True, True, False,
        "Codice non nel catalogo standard"
    ))


def get_domain_label(code: str) -> str:
    return classify_doc(code).domain.value


def is_logistics(code: str) -> bool:
    return classify_doc(code).domain == DocDomain.LOGISTICA


def contributes_to_saving(code: str) -> bool:
    return classify_doc(code).contributes_saving
