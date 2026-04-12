SUPPORTED_FILE_FAMILIES = [
    "saving_orders",
    "detailed_orders",
    "resources_team",
    "cycle_times",
    "non_conformities",
    "suppliers_master",
]

MANDATORY_CANONICAL_FIELDS = {
    "saving_orders": ["supplier_name", "document_type", "document_date", "committed_amount"],
    "detailed_orders": ["supplier_name", "document_type"],
    "resources_team": ["resource_name"],
    "cycle_times": ["duration_days"],
    "non_conformities": ["supplier_name"],
    "suppliers_master": ["supplier_name"],
}
