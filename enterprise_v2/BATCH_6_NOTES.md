# Batch 6 Notes

## Fix principale
Il backend /inspect-excel funziona correttamente da Swagger.
Il frontend falliva su upload perché il valore VITE_API_BASE_URL non risultava affidabile nel build Docker/Vite.

## Correzione
- introdotto src/config.js
- URL backend impostato direttamente per la fase di test
- tutte le pagine frontend usano API_BASE centralizzato

## Obiettivo
Sbloccare subito i test reali di caricamento Excel.
