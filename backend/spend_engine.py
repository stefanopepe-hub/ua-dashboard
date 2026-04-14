"""
spend_engine.py — Classificatore deterministico della spesa
Fondazione Telethon ETS — UA Dashboard Enterprise

Bucket: MATERIALI | SERVIZI | STRUMENTAZIONE | NON CLASSIFICATO
Priorità: macro_categoria > desc_gruppo_merceol > regole esplicite > fallback
"""
from __future__ import annotations
from enum import Enum
from typing import Optional
import re


class SpendBucket(str, Enum):
    MATERIALI      = "Materiali di Consumo"
    SERVIZI        = "Servizi"
    STRUMENTAZIONE = "Strumentazione"
    NON_CLASS      = "Non Classificato"


# Regole per macro_categoria (match esatto case-insensitive)
_MACRO_RULES: dict[str, SpendBucket] = {
    "pharma":          SpendBucket.MATERIALI,
    "ricerca":         SpendBucket.MATERIALI,
    "laboratorio":     SpendBucket.MATERIALI,
    "reagenti":        SpendBucket.MATERIALI,
    "raccolta fondi":  SpendBucket.SERVIZI,
    "risorse umane":   SpendBucket.SERVIZI,
    "it & dati":       SpendBucket.SERVIZI,
    "it":              SpendBucket.SERVIZI,
    "corporate":       SpendBucket.SERVIZI,
    "governance":      SpendBucket.SERVIZI,
    "facility":        SpendBucket.SERVIZI,
    "strumentazione":  SpendBucket.STRUMENTAZIONE,
    "equipment":       SpendBucket.STRUMENTAZIONE,
    "attrezzature":    SpendBucket.STRUMENTAZIONE,
}

# Regole per desc_gruppo_merceol (keyword matching)
_CATEGORY_KEYWORDS: list[tuple[list[str], SpendBucket]] = [
    # Materiali
    (["reagent", "chemical", "plastic", "consumab", "materiale", "reagente",
      "chimic", "plastica", "vetro", "glassware", "cell culture", "coltura",
      "antibod", "anticorp", "kit", "buffer", "medium", "media"], SpendBucket.MATERIALI),
    # Strumentazione
    (["strumentaz", "equipment", "macchinar", "apparecch", "instrument",
      "microscop", "centrifug", "autoclave", "freezer", "incubator",
      "flow cytom", "pcr", "sequenziator", "robot"], SpendBucket.STRUMENTAZIONE),
    # Servizi
    (["serviz", "service", "consulenz", "consultan", "manutenz", "maintenan",
      "trasfert", "travel", "formaz", "training", "software", "licenz",
      "licens", "abbonament", "subscription", "pulizia", "cleaning",
      "telemarket", "notai", "assicuraz", "insurance", "ristorazi",
      "catering", "brevett", "patent", "publicaz", "publication",
      "convegno", "congress"], SpendBucket.SERVIZI),
]


def classify_spend(
    macro_cat: Optional[str],
    group_desc: Optional[str],
) -> SpendBucket:
    """
    Classifica la spesa in un bucket.
    Deterministico: stesso input → stesso output.
    """
    # Priority 1: macro_categoria
    if macro_cat:
        clean = macro_cat.strip().lower()
        for key, bucket in _MACRO_RULES.items():
            if key in clean:
                return bucket

    # Priority 2: group description keyword matching
    if group_desc:
        clean = group_desc.strip().lower()
        for keywords, bucket in _CATEGORY_KEYWORDS:
            if any(kw in clean for kw in keywords):
                return bucket

    return SpendBucket.NON_CLASS


def classify_spend_label(macro: Optional[str], group: Optional[str]) -> str:
    return classify_spend(macro, group).value
