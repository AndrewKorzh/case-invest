INSPECTIONS_REGISTER = [
    {
        "table_name": "product_type",
        "primary_key": "product_type_cd",
        "inspections": [
            {"type":"UNIQUE",
            "collumn":"product_type_cd"}
        ]
    },
    {
        "table_name": "crm_account_status",
        "primary_key": "account_status_cd",
        "inspections": [
            {"type":"UNIQUE",
            "collumn":"account_status_cd"}
        ]
    },
    {
        "table_name": "crm_account_type",
        "primary_key": "account_type_cd",
        "inspections": [
            {"type":"UNIQUE",
            "collumn":"account_type_cd"}
        ]
    },
    {
        "table_name": "crm_transaction_type",
        "primary_key": "transaction_type_cd",
        "inspections": [
            {"type":"UNIQUE",
            "collumn":"transaction_type_cd"}
        ]
    },
    {
        "table_name": "service_request_status",
        "primary_key": "service_request_status_cd",
        "inspections": [
            {"type":"UNIQUE",
            "collumn":"service_request_status_cd"}
        ]
    },
    {
        "table_name": "service_request_type",
        "primary_key": "service_request_type_cd",
        "inspections": [
            {"type":"UNIQUE",
            "collumn":"service_request_type_cd"}
        ]
    },
    {
        "table_name": "application",
        "primary_key": "application_id",
        "inspections": [
            {"type":"UNIQUE",
            "collumn":"application_id"}
        ]
    },
    {
        "table_name": "advert_source",
        "primary_key": "advert_source_id",
        "inspections": [
            {"type":"UNIQUE",
            "collumn":"advert_source_id"}
        ]
    },
    {
        "table_name": "crm_account",
        "primary_key": "account_id",
        "inspections": [
            {"type":"UNIQUE",
            "collumn":"account_id"}
        ]
    },
    {
        "table_name": "service_request",
        "primary_key": "service_request_id",
        "inspections": [
            {"type":"UNIQUE",
            "collumn":"service_request_id"}
        ]
    },
    {
        "table_name": "crm_customer",
        "primary_key": "crm_customer_id",
        "inspections": [
            {"type":"UNIQUE",
            "collumn":"crm_customer_id"}
        ]
    },
    {
        "table_name": "cab_customer",
        "primary_key": "customer_id",
        "inspections": [
            {"type":"UNIQUE",
            "collumn":"customer_id"}
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