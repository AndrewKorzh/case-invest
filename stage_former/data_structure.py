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
    # "service_request": {
    #     "dir_name":"service_request",
    #     "table_name":"service_request",
    #     "headers": {"service_request_id":"int",
    #                 "customer_id":"int",
    #                 "service_request_type_cd":"int",
    #                 "service_request_status_cd":"int",
    #                 "tail_limit":"int",
    #                 "create_dttm":"text",
    #                 "delete_dttm":"text"}
    # },
    # "crm_transaction":{
    #     "dir_name":"crm_transaction",
    #     "table_name":"crm_transaction",
    #     "headers": {"transaction_id":"int",
    #                 "orig_id":"int",
    #                 "account_id":"int",
    #                 "transaction_type_cd":"int",
    #                 "transaction_amt":"double",
    #                 "transaction_dttm":"datetime",
    #                 "create_dttm":"datetime",
    #                 "delete_dttm":"text"}
    # }

    # account_status_cd|account_status_nm|
    
}