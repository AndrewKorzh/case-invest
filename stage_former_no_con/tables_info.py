import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from config import STAGE_SCHEMA_NAME

TABLES_INFO = [
    {
    "dir_name":"product_type",
    "table_name":"product_type",
    "headers": {"product_type_cd":{"type":"integer"},
                "product_type_nm":{"type":"text"}}
    },
    {
    "dir_name":"crm_account_status",
    "table_name":"crm_account_status",
    "headers": {"account_status_cd":{"type":"integer"},
                "account_status_nm":{"type":"text"}}
    },
    {
    "dir_name":"advert_source",
    "table_name":"advert_source",
    "headers": {"advert_source_id":{"type":"integer"},
                "advert_source_nm":{"type":"text"},
                "monthly_payment_amt":{"type":"decimal(12,6)"},
                "start_month":{"type":"integer"},
                "end_month":{"type":"integer"},
                "create_dttm":{"type":"timestamp"},
                "delete_dttm":{"type":"timestamp"}}       
    },
    {
    "dir_name":"crm_transaction_type",
    "table_name":"crm_transaction_type",
    "headers": {"transaction_type_cd":{"type":"integer"},
                "transaction_type_nm":{"type":"text"}}
    },
    {
    "dir_name":"crm_account_type",
    "table_name":"crm_account_type",
    "headers": {"account_type_cd":{"type":"integer"},
                "account_type_nm":{"type":"text"}}
    },
    {
    "dir_name":"service_request_status",
    "table_name":"service_request_status",
    "headers": {"service_request_status_cd":{"type":"integer"},
                "service_request_status_nm":{"type":"text"}}
    },
    {
    "dir_name":"service_request_type",
    "table_name":"service_request_type",
    "headers": {"service_request_type_cd":{"type":"integer"},
                "service_request_type_nm":{"type":"text"}}
    },

    {
    "dir_name":"cab_customer",
    "table_name":"cab_customer",
    "headers": {"customer_id":{"type":"integer"},
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
    {
    "dir_name":"service_request",
    "table_name":"service_request",
    "headers": {"service_request_id":{"type":"integer"},
                "customer_id":{"type":"integer"},
                "service_request_type_cd":{"type":"integer"},
                "service_request_status_cd":{"type":"integer"},
                "tail_limit":{"type":"integer"},
                "create_dttm":{"type":"timestamp"},
                "delete_dttm":{"type":"timestamp"},}
    },

    {
    "dir_name":"crm_customer",
    "table_name":"crm_customer",
    "headers": {"crm_customer_id":{"type":"integer"},
                "customer_id":{"type":"integer"},
                "birth_dt":{"type":"timestamp"},
                "phone_num":{"type":"text"},
                "email":{"type":"text"},                   
                "first_nm":{"type":"text"},
                "last_nm":{"type":"text"},
                "create_dttm":{"type":"timestamp"},
                "delete_dttm":{"type":"timestamp"}}  
    },
    {
    "dir_name":"application",
    "table_name":"application",
    "headers": {"application_id":{"type":"integer"},
                "product_type_cd":{"type":"integer"},
                "customer_id":{"type":"integer"},
                "advert_source_id":{"type":"integer"},
                "create_dttm":{"type":"timestamp"},
                "delete_dttm":{"type":"timestamp"},}
    },
    {
    "dir_name":"crm_account",
    "table_name":"crm_account",
    "headers": {"account_id":{"type":"integer"},
                "account_type_cd":{"type":"integer"},
                "acccount_create_dt":{"type":"date"},
                "account_status_cd":{"type":"integer"},
                "application_id":{"type":"integer"},
                "create_dttm":{"type":"timestamp"},
                "delete_dttm":{"type":"timestamp"},}
    },
    {
    "dir_name":"crm_transaction",
    "table_name":"crm_transaction",
    "headers": {"transaction_id":{"type":"integer"},
                "orig_id":{"type":"integer"},
                "account_id":{"type":"integer"},
                "transaction_type_cd":{"type":"integer"},
                "transaction_amt":{"type":"decimal(12,6)"},
                "transaction_dttm":{"type":"timestamp"},
                "create_dttm":{"type":"timestamp"},
                "delete_dttm":{"type":"timestamp"}}
    }
]

ERROR_LOG_TABLE_NAME = "error_log"

SERVICE_TABLES = [
    {
    "table_name":ERROR_LOG_TABLE_NAME,
    "query": f"""
            CREATE TABLE IF NOT EXISTS {STAGE_SCHEMA_NAME}.{ERROR_LOG_TABLE_NAME} (
                error_id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                id INTEGER NOT NULL,
                error_type TEXT NOT NULL,
                error_message TEXT NOT NULL,
                error_dttm TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """
    }

]

# 