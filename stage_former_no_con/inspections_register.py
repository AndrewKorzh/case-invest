INSPECTIONS_REGISTER = [
    # {
    #     "table_name": "crm_transaction",
    #     "primary_key": "transaction_id",
    #     "inspections": [
    #         {"UNIQUE":"transaction_id"}
    #     ]
    # },

    {
        "table_name": "product_type",
        "primary_key": "product_type_cd",
        "inspections": [
            {"type":"UNIQUE",
            "collumn":"product_type_cd"}
        ]
    },
    {
        "table_name": "crm_transaction",
        "primary_key": "transaction_id",
        "inspections": [
            {"type":"UNIQUE",
            "collumn":"transaction_id"}
        ]
    }
]