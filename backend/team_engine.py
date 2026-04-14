"""
team_engine.py — Normalizzazione team procurement
Fondazione Telethon ETS — UA Dashboard Enterprise

Struttura ufficiale dell'ufficio acquisti con normalization deterministica.
"""
from __future__ import annotations
import re
from typing import Optional

# ── Struttura ufficiale ────────────────────────────────────────────────────────
MANAGERS = ["Stefano Pepe", "Francesco Di Clemente"]

BUYERS = [
    "Silvana Ruotolo",
    "Marina Padricelli",
    "Luisa Veneruso",
    "Katuscia Leonardi",
    "Francesca Perazzetti",
    "Loredana Scialanga",
    "Mariacarla Di Matteo",
    "Luca Monti",
]

MANAGER_ASSIGNMENTS: dict[str, list[str]] = {
    "Stefano Pepe":       ["Luca Monti", "Marina Padricelli", "Silvana Ruotolo", "Luisa Veneruso"],
    "Francesco Di Clemente": ["Katuscia Leonardi", "Francesca Perazzetti"],
}

AUTONOMOUS = ["Loredana Scialanga", "Mariacarla Di Matteo"]

ALL_TEAM = MANAGERS + BUYERS

# Varianti note (alias → nome canonico)
_ALIASES: dict[str, str] = {
    "pepe":            "Stefano Pepe",
    "stefano pepe":    "Stefano Pepe",
    "s.pepe":          "Stefano Pepe",
    "spepe":           "Stefano Pepe",
    "di clemente":     "Francesco Di Clemente",
    "diclemente":      "Francesco Di Clemente",
    "f.di clemente":   "Francesco Di Clemente",
    "ruotolo":         "Silvana Ruotolo",
    "silvana ruotolo": "Silvana Ruotolo",
    "ruotolo silvana": "Silvana Ruotolo",
    "padricelli":      "Marina Padricelli",
    "marina padricelli":"Marina Padricelli",
    "padricelli marina":"Marina Padricelli",
    "veneruso":        "Luisa Veneruso",
    "luisa veneruso":  "Luisa Veneruso",
    "leonardi":        "Katuscia Leonardi",
    "katuscia leonardi":"Katuscia Leonardi",
    "leonardi katuscia":"Katuscia Leonardi",
    "perazzetti":      "Francesca Perazzetti",
    "francesca perazzetti":"Francesca Perazzetti",
    "perazzetti francesca":"Francesca Perazzetti",
    "scialanga":       "Loredana Scialanga",
    "loredana scialanga":"Loredana Scialanga",
    "di matteo":       "Mariacarla Di Matteo",
    "dimatteo":        "Mariacarla Di Matteo",
    "mariacarla di matteo":"Mariacarla Di Matteo",
    "monti":           "Luca Monti",
    "luca monti":      "Luca Monti",
    "monti luca":      "Luca Monti",
}

# Label da escludere (non sono persone reali)
_NOISE = frozenset({
    "", "nan", "none", "n/a", "n.a.", "ufficio acquisti", "acquisti",
    "procurement", "admin", "sistema", "system", "test",
})


def normalize_name(raw: str | None) -> Optional[str]:
    """
    Normalizza un nome utente al formato canonico.
    Restituisce None se non riconoscibile o rumore.
    """
    if not raw:
        return None
    clean = str(raw).strip()
    if clean.lower() in _NOISE:
        return None

    # Prova lookup diretto
    key = clean.lower().strip()
    if key in _ALIASES:
        return _ALIASES[key]

    # Prova normalizzazione spazi multipli
    clean = re.sub(r'\s+', ' ', clean)
    key = clean.lower()
    if key in _ALIASES:
        return _ALIASES[key]

    # Prova solo cognome (prima parola se formato "Cognome Nome")
    parts = clean.split()
    if len(parts) >= 2:
        # Prova "Nome Cognome" invertito
        inverted = f"{parts[-1]} {' '.join(parts[:-1])}".lower()
        if inverted in _ALIASES:
            return _ALIASES[inverted]
        # Solo cognome
        surname_key = parts[0].lower()
        if surname_key in _ALIASES:
            return _ALIASES[surname_key]

    # Se è nel team con case diverso
    for member in ALL_TEAM:
        if member.lower() == key:
            return member

    # Non riconosciuto — restituisce il valore originale pulito
    return clean if clean else None


def get_manager(buyer_name: str) -> Optional[str]:
    """Restituisce il manager di un buyer."""
    canonical = normalize_name(buyer_name)
    if not canonical:
        return None
    for manager, reports in MANAGER_ASSIGNMENTS.items():
        if canonical in reports:
            return manager
    if canonical in AUTONOMOUS:
        return "Autonomo"
    if canonical in MANAGERS:
        return "Management"
    return None


def is_team_member(name: str) -> bool:
    """True se il nome è riconoscibile come membro del team."""
    return normalize_name(name) is not None


def team_summary() -> dict:
    """Restituisce la struttura del team come dizionario."""
    return {
        "managers": MANAGERS,
        "buyers": BUYERS,
        "autonomous": AUTONOMOUS,
        "assignments": MANAGER_ASSIGNMENTS,
        "total": len(ALL_TEAM),
    }
