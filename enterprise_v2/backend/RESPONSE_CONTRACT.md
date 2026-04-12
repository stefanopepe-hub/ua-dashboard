# Response Contract

## /health
{
  "ok": true,
  "service": "enterprise_v2_backend"
}

## /inspect-columns
{
  "file_family": "saving_orders",
  "normalized_columns": [],
  "mapped_fields": {},
  "readiness": {
    "available_fields": [],
    "missing_required_fields": []
  },
  "confidence_score": 0.0
}

## /inspect-excel
{
  "ok": true,
  "file_name": "example.xlsx",
  "file_family": "saving_orders",
  "normalized_columns": [],
  "mapped_fields": {},
  "readiness": {
    "available_fields": [],
    "missing_required_fields": []
  },
  "confidence_score": 0.0,
  "sheet_names": [],
  "selected_sheet": "Foglio1",
  "selected_header_row": 0,
  "sheets": []
}

## /analytics/saving/*
{
  "ok": true,
  "data": []
}

## Regola
Le risposte devono avere shape stabile.
Il frontend non deve mai ricevere strutture inattese.
