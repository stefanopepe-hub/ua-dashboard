# Testing Notes

## Health check
GET /health

## Inspect columns
POST /inspect-columns

## Payload di test
Usare il file manual_test_payload.json

## Obiettivo
Verificare che il backend:
- riconosca la famiglia file
- normalizzi le colonne
- mappi i campi canonici
- calcoli la readiness
