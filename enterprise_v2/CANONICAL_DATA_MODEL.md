# Canonical Data Model

## Obiettivo
Tutte le analytics devono leggere solo dati normalizzati, mai direttamente i file Excel raw.

## Entità principali
- uploads_audit
- upload_jobs
- analytics_readiness
- procurement_documents
- procurement_lines
- savings_records
- suppliers_master
- supplier_accreditation
- non_conformities
- cycle_time_records
- resource_performance_records
- dim_cdc
- dim_buyer
- dim_protocol
- dim_category
- dim_business_unit
- dim_currency

## Principio
Ogni file caricato deve essere:
1. classificato
2. mappato
3. normalizzato
4. validato
5. pubblicato solo se stabile
