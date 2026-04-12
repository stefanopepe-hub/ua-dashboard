# Response Contract

## /health
{
  "ok": true,
  "service": "enterprise_v2_backend"
}

## /inspect-columns e /inspect-excel
{
  "file_family": "saving_orders",
  "normalized_columns": [],
  "mapped_fields": {},
  "readiness": {
    "available_fields": [],
    "missing_required_fields": []
  },
  "confidence_score": 0.0,
  "sheet_names": [],
  "file_name": "example.xlsx"
}

## Regola
Le risposte devono avere shape stabile.
Il frontend non deve mai ricevere strutture inattese.
