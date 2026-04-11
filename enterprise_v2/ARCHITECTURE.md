# Enterprise V2 Architecture

## Obiettivo
Separare completamente la nuova piattaforma enterprise dall'app attuale.

## Struttura
- enterprise_v2/backend
- enterprise_v2/frontend
- enterprise_v2/infra

## Principi
- architettura modulare
- contratti API stabili
- modello dati canonico
- import Excel adattivo
- validazione e data quality prima delle analytics
- test e release gate obbligatori

## Flusso dati
1. Upload file Excel
2. Riconoscimento famiglia file
3. Riconoscimento foglio e header
4. Mapping colonne
5. Normalizzazione nel modello canonico
6. Data Quality / Readiness
7. Analytics
8. Reporting

## Famiglie file supportate
- saving / ordini
- ordini dettagliati
- risorse / team
- tempi attraversamento
- non conformità
- anagrafica / albo fornitori

## Regola fondamentale
La nuova piattaforma non deve rompere l'app attuale.
