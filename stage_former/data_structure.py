DATA_STRUCTURE = {
    "product_type": {
        "dir_name":"product_type",
        "table_name":"product_type",
        "headers": {"product_type_cd":{"type":"integer", "role":"PK"},
                    "product_type_nm":{"type":"text"}}
    },
    "crm_account_status": {
    "dir_name":"crm_account_status",
    "table_name":"crm_account_status",
    "headers": {"account_status_cd":{"type":"integer", "role":"PK"},
                "account_status_nm":{"type":"text"}}
    },
    "advert_source": {
        "dir_name":"advert_source",
        "table_name":"advert_source",
        "headers": {"advert_source_id":{"type":"integer", "role":"PK"},
                    "advert_source_nm":{"type":"text"},
                    "monthly_payment_amt":{"type":"decimal(12,6)"},
                    "start_month":{"type":"integer"},
                    "end_month":{"type":"integer"},
                    "create_dttm":{"type":"timestamp"},
                    "delete_dttm":{"type":"timestamp"}}       
    },
    "crm_transaction_type": {
    "dir_name":"crm_transaction_type",
    "table_name":"crm_transaction_type",
    "headers": {"transaction_type_cd":{"type":"integer", "role":"PK"},
                "transaction_type_nm":{"type":"text"}}
    },
    "crm_account_type": {
    "dir_name":"crm_account_type",
    "table_name":"crm_account_type",
    "headers": {"account_type_cd":{"type":"integer", "role":"PK"},
                "account_type_nm":{"type":"text"}}
    },

    "service_request_status": {
    "dir_name":"service_request_status",
    "table_name":"service_request_status",
    "headers": {"service_request_status_cd":{"type":"integer", "role":"PK"},
                "service_request_status_nm":{"type":"text"}}
    },
    "service_request_type": {
    "dir_name":"service_request_type",
    "table_name":"service_request_type",
    "headers": {"service_request_type_cd":{"type":"integer", "role":"PK"},
                "service_request_type_nm":{"type":"text"}}
    },

    "cab_customer": {
        "dir_name":"cab_customer",
        "table_name":"cab_customer",
        "headers": {"customer_id":{"type":"integer", "role":"PK"},
                    "birth_dt":{"type":"date"},
                    "passport_num":{"type":"text"},
                    "phone_num":{"type":"text"},
                    "add_phone_num":{"type":"text"},
                    "email":{"type":"text"},
                    "reg_address_txt":{"type":"text"},
                    "fact_address_txt":{"type":"text"},
                    "first_nm":{"type":"text"},
                    "last_nm":{"type":"text"},
                    "middle_nm":{"type":"text"},
                    "create_dttm":{"type":"timestamp"},
                    "delete_dttm":{"type":"timestamp"}}       
    },
    "service_request": {
        "dir_name":"service_request",
        "table_name":"service_request",
        "headers": {"service_request_id":{"type":"integer", "role":"PK"},
                    "customer_id":{"type":"integer"},
                    "service_request_type_cd":{"type":"integer"},
                    "service_request_status_cd":{"type":"integer"},
                    "tail_limit":{"type":"integer"},
                    "create_dttm":{"type":"timestamp"},
                    "delete_dttm":{"type":"timestamp"},}
    },

    "crm_customer": {
        "dir_name":"crm_customer",
        "table_name":"crm_customer",
        "headers": {"crm_customer_id":{"type":"integer", "role":"PK"},
                    "customer_id":{"type":"integer"},
                    "birth_dt":{"type":"timestamp"},
                    "phone_num":{"type":"text"},
                    "email":{"type":"text"},                   
                    "first_nm":{"type":"text"},
                    "last_nm":{"type":"text"},
                    "create_dttm":{"type":"timestamp"},
                    "delete_dttm":{"type":"timestamp"}}  
    },
    "application": {
        "dir_name":"application",
        "table_name":"application",
        "headers": {"application_id":{"type":"integer", "role":"PK"},
                    "product_type_cd":{"type":"integer"},
                    "customer_id":{"type":"integer"},
                    "advert_source_id":{"type":"integer"},
                    "create_dttm":{"type":"timestamp"},
                    "delete_dttm":{"type":"timestamp"},}
    },
    "crm_account": {
        "dir_name":"crm_account",
        "table_name":"crm_account",
        "headers": {"account_id":{"type":"integer", "role":"PK"},
                    "account_type_cd":{"type":"integer"},
                    "acccount_create_dt":{"type":"date"},
                    "account_status_cd":{"type":"integer"},
                    "application_id":{"type":"integer"},
                    "create_dttm":{"type":"timestamp"},
                    "delete_dttm":{"type":"timestamp"},}
    },
    "crm_transaction":{
        "dir_name":"crm_transaction",
        "table_name":"crm_transaction",
        "headers": {"transaction_id":{"type":"integer", "role":"PK"},
                    "orig_id":{"type":"integer"},
                    "account_id":{"type":"integer"},
                    "transaction_type_cd":{"type":"integer"},
                    "transaction_amt":{"type":"decimal(12,6)"},
                    "transaction_dttm":{"type":"timestamp"},
                    "create_dttm":{"type":"timestamp"},
                    "delete_dttm":{"type":"timestamp"}}
    }
}