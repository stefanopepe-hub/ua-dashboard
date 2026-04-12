SUPPORTED_FILE_FAMILIES = [
    "saving_orders",
    "detailed_orders",
    "resources_team",
    "cycle_times",
    "non_conformities",
    "suppliers_master",
]

MANDATORY_CANONICAL_FIELDS = {
    "saving_orders": [
        "supplier_name",
        "document_type",
        "document_date",
        "committed_amount",
    ],
    "detailed_orders": [
        "supplier_name",
        "document_type",
        "document_date",
        "item_description",
        "quantity",
    ],
    "resources_team": [
        "resource_name",
    ],
    "cycle_times": [
        "protocol_reference",
        "total_days",
        "bottleneck",
    ],
    "non_conformities": [
        "supplier_name",
        "origin_date",
        "nc_flag",
    ],
    "suppliers_master": [
        "supplier_name",
    ],
}
