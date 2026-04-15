"""
services/fx_rates.py — Historical FX Rate Engine v1.0
Fondazione Telethon ETS — UA Dashboard

Fornisce cambi storici EURO-corrispondente per conversione importi non-EUR.
Sorgente primaria: BCE Data Portal (SDMX-JSON)
Fallback: interpolazione sul giorno lavorativo più vicino (max ±10 giorni)

Uso:
    from services.fx_rates import get_rate
    rate = get_rate("USD", date(2026, 3, 15))  # → es. 1.0834
    eur_amount = usd_amount / rate
"""
import logging
import datetime
from typing import Dict, Optional, Tuple
import httpx

log = logging.getLogger("ua.fx_rates")

# ── Cache in-memory ──────────────────────────────────────────────
# {(currency_upper, date_iso): rate_float}
_CACHE: Dict[Tuple[str, str], float] = {}

# Currencies known to never need conversion
_EUR_ALIASES = {"EUR", "EURO", "€"}

# ECB API endpoint template
_ECB_URL = (
    "https://data-api.ecb.europa.eu/service/data/EXR/"
    "D.{currency}.EUR.SP00.A"
    "?startPeriod={start}&endPeriod={end}&format=jsondata"
)

# HTTP client (timeout generoso per evitare flap)
_TIMEOUT = httpx.Timeout(12.0)


def _fetch_ecb(currency: str, date_from: datetime.date, date_to: datetime.date) -> Dict[str, float]:
    """
    Interroga l'ECB Data Portal e restituisce un dict {date_iso: rate}.
    Il tasso ECB è espresso come unità di valuta straniera per 1 EUR.
    Noi vogliamo: EUR → valuta, quindi la formula è:
        eur_amount = foreign_amount / rate
    """
    url = _ECB_URL.format(
        currency=currency.upper(),
        start=date_from.isoformat(),
        end=date_to.isoformat(),
    )
    try:
        r = httpx.get(url, timeout=_TIMEOUT, follow_redirects=True)
        if r.status_code != 200:
            log.warning(f"ECB API HTTP {r.status_code} for {currency} {date_from}–{date_to}")
            return {}
        data = r.json()
        # Struttura SDMX-JSON: dataSets[0].series -> "0:0:0:0:0" -> observations -> {"0": [value,...], ...}
        # Le date sono in structure.dimensions.observation[0].values[index].id
        try:
            dims = data["structure"]["dimensions"]["observation"][0]["values"]
            obs  = data["dataSets"][0]["series"]["0:0:0:0:0"]["observations"]
        except (KeyError, IndexError, TypeError):
            log.debug(f"ECB response unexpected structure for {currency}")
            return {}

        result = {}
        for idx_str, vals in obs.items():
            idx = int(idx_str)
            if idx < len(dims) and vals and vals[0] is not None:
                date_id = dims[idx]["id"]    # e.g. "2026-03-15"
                result[date_id] = float(vals[0])
        return result
    except Exception as e:
        log.warning(f"ECB fetch failed for {currency}: {e}")
        return {}


def get_rate(currency: str, date: datetime.date) -> float:
    """
    Restituisce il cambio BCE: unità di `currency` per 1 EUR.
    Quindi per convertire: eur = amount_in_currency / get_rate(currency, date)

    - Ritorna 1.0 per EUR/EURO/€
    - Cache in-memory per la sessione corrente
    - Fallback al giorno lavorativo più vicino (max 10 giorni indietro)
    """
    if not currency:
        return 1.0
    cur = currency.strip().upper()
    if cur in _EUR_ALIASES:
        return 1.0

    date_iso = date.isoformat()
    cache_key = (cur, date_iso)
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    # Finestra di ricerca: dal giorno richiesto a 14 giorni prima
    # (copre weekend + festività + eventuali gaps BCE)
    date_from = date - datetime.timedelta(days=14)
    rates = _fetch_ecb(cur, date_from, date)

    if not rates:
        log.warning(f"No ECB rate found for {cur} around {date_iso}, using 1.0")
        _CACHE[cache_key] = 1.0
        return 1.0

    # Prende il tasso più recente disponibile entro la finestra
    best_date = max(rates.keys())
    rate = rates[best_date]

    # Popola cache per tutte le date trovate
    for d_str, r in rates.items():
        _CACHE[(cur, d_str)] = r

    # Cache anche la data originale richiesta col tasso trovato
    if date_iso not in rates:
        _CACHE[cache_key] = rate

    log.debug(f"FX {cur}/{date_iso}: {rate} (from ECB date {best_date})")
    return rate


def get_rate_safe(currency: str, date: datetime.date, fallback: float = 1.0) -> float:
    """
    Wrapper sicuro: ritorna `fallback` in caso di qualsiasi errore.
    """
    try:
        return get_rate(currency, date)
    except Exception as e:
        log.warning(f"get_rate_safe error for {currency}/{date}: {e}")
        return fallback


def clear_cache() -> None:
    """Svuota la cache in-memory (utile in test e per reload dati)."""
    _CACHE.clear()


def cache_stats() -> dict:
    """Statistiche cache per diagnostica."""
    return {
        "entries": len(_CACHE),
        "currencies": list({k[0] for k in _CACHE}),
    }
