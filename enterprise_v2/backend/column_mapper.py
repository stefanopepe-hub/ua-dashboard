from typing import Dict, Iterable

from column_normalizer import normalize_column_name
from column_synonyms import COLUMN_SYNONYMS


def map_columns_to_canonical(column_names: Iterable[str]) -> Dict[str, str]:
    normalized_source = {original: normalize_column_name(original) for original in column_names}
    result: Dict[str, str] = {}

    for canonical_field, synonyms in COLUMN_SYNONYMS.items():
        normalized_synonyms = {normalize_column_name(s) for s in synonyms}
        for original_name, normalized_name in normalized_source.items():
            if normalized_name in normalized_synonyms:
                result[canonical_field] = original_name
                break

    # Generic semantic fallbacks
    for original_name, normalized_name in normalized_source.items():
        if "committed_amount" not in result and "negoziato" in normalized_name:
            result["committed_amount"] = original_name
        if "list_amount" not in result and "iniziale" in normalized_name:
            result["list_amount"] = original_name
        if "document_date" not in result and normalized_name in {"data doc", "data documento"}:
            result["document_date"] = original_name
        if "protocol_commessa" not in result and "commessa" in normalized_name:
            result["protocol_commessa"] = original_name
        if "protocol_order" not in result and ("ordine" in normalized_name and "protoc" in normalized_name):
            result["protocol_order"] = original_name

        # Detailed orders
        if "supplier_name" not in result and "ragione sociale anagrafica" in normalized_name:
            result["supplier_name"] = original_name
        if "document_type" not in result and normalized_name in {"cod documento", "tipo origine"}:
            result["document_type"] = original_name
        if "item_description" not in result and "descrizione articolo" in normalized_name:
            result["item_description"] = original_name
        if "quantity" not in result and ("qta 1 doc" in normalized_name or normalized_name == "qty"):
            result["quantity"] = original_name

        # Cycle times
        if "protocol_reference" not in result and normalized_name == "protocol":
            result["protocol_reference"] = original_name
        if "total_days" not in result and normalized_name == "total days":
            result["total_days"] = original_name
        if "bottleneck" not in result and normalized_name == "bottleneck":
            result["bottleneck"] = original_name
        if "days_purchasing" not in result and normalized_name == "days purchasing":
            result["days_purchasing"] = original_name
        if "days_auto" not in result and normalized_name == "days auto":
            result["days_auto"] = original_name

        # Non conformities
        if "origin_date" not in result and normalized_name == "data origine":
            result["origin_date"] = original_name
        if "origin_user" not in result and normalized_name == "utente origine":
            result["origin_user"] = original_name
        if "nc_flag" not in result and normalized_name == "non conformità":
            result["nc_flag"] = original_name
        if "invoice_amount" not in result and normalized_name == "importo prima fattura":
            result["invoice_amount"] = original_name

    return result
