# Testing Notes

## Health check
GET /health

## Inspect columns
POST /inspect-columns

## Inspect real Excel
POST /inspect-excel

## Payload di test manuale
Usare il file manual_test_payload.json

## Obiettivo
Verificare che il backend:
- riconosca la famiglia file
- normalizzi le colonne
- mappi i campi canonici
- calcoli la readiness
- legga un file Excel reale caricato
