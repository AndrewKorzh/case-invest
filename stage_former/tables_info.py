import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from config import STAGE_SCHEMA_NAME

TABLES_INFO = [
    {
    "dir_name":"product_type",
    "table_name":"product_type",
    "headers": {"product_type_cd":"integer",
                "product_type_nm":"text"}
    },
    {
    "dir_name":"crm_account_status",
    "table_name":"crm_account_status",
    "headers": {"account_status_cd":"integer",
                "account_status_nm":"text"}
    },
    {
    "dir_name":"advert_source",
    "table_name":"advert_source",
    "headers": {"advert_source_id":"integer",
                "advert_source_nm":"text",
                "monthly_payment_amt":"decimal(12,6)",
                "start_month":"integer",
                "end_month":"integer",
                "create_dttm":"timestamp",
                "delete_dttm":"timestamp"}       
    },
    {
    "dir_name":"crm_transaction_type",
    "table_name":"crm_transaction_type",
    "headers": {"transaction_type_cd":"integer",
                "transaction_type_nm":"text"}
    },
    {
    "dir_name":"crm_account_type",
    "table_name":"crm_account_type",
    "headers": {"account_type_cd":"integer",
                "account_type_nm":"text"}
    },
    {
    "dir_name":"service_request_status",
    "table_name":"service_request_status",
    "headers": {"service_request_status_cd":"integer",
                "service_request_status_nm":"text"}
    },
    {
    "dir_name":"service_request_type",
    "table_name":"service_request_type",
    "headers": {"service_request_type_cd":"integer",
                "service_request_type_nm":"text"}
    },

    {
    "dir_name":"cab_customer",
    "table_name":"cab_customer",
    "headers": {"customer_id":"integer",
                "birth_dt":"date",
                "passport_num":"text",
                "phone_num":"text",
                "add_phone_num":"text",
                "email":"text",
                "reg_address_txt":"text",
                "fact_address_txt":"text",
                "first_nm":"text",
                "last_nm":"text",
                "middle_nm":"text",
                "create_dttm":"timestamp",
                "delete_dttm":"timestamp"}       
    },
    {
    "dir_name":"service_request",
    "table_name":"service_request",
    "headers": {"service_request_id":"integer",
                "customer_id":"integer",
                "service_request_type_cd":"integer",
                "service_request_status_cd":"integer",
                "tail_limit":"integer",
                "create_dttm":"timestamp",
                "delete_dttm":"timestamp",}
    },

    {
    "dir_name":"crm_customer",
    "table_name":"crm_customer",
    "headers": {"crm_customer_id":"integer",
                "customer_id":"integer",
                "birth_dt":"timestamp",
                "phone_num":"text",
                "email":"text",                   
                "first_nm":"text",
                "last_nm":"text",
                "create_dttm":"timestamp",
                "delete_dttm":"timestamp"}  
    },
    {
    "dir_name":"application",
    "table_name":"application",
    "headers": {"application_id":"integer",
                "product_type_cd":"integer",
                "customer_id":"integer",
                "advert_source_id":"integer",
                "create_dttm":"timestamp",
                "delete_dttm":"timestamp",}
    },
    {
    "dir_name":"crm_account",
    "table_name":"crm_account",
    "headers": {"account_id":"integer",
                "account_type_cd":"integer",
                "acccount_create_dt":"date",
                "account_status_cd":"integer",
                "application_id":"integer",
                "create_dttm":"timestamp",
                "delete_dttm":"timestamp",}
    },
    {
    "dir_name":"crm_transaction",
    "table_name":"crm_transaction",
    "headers": {"transaction_id":"integer",
                "orig_id":"integer",
                "account_id":"integer",
                "transaction_type_cd":"integer",
                "transaction_amt":"decimal(12,6)",
                "transaction_dttm":"timestamp",
                "create_dttm":"timestamp",
                "delete_dttm":"timestamp"}
    }
]

ERROR_LOG_TABLE_NAME = "error_log"
BAD_SOURCE_TABLE_NAME = "bad_source"
DATA_UPDATE_TABLE_NAME = "data_update"
LOADED_AND_LOS_TABLE_NAME = "loaded_and_lost_data"

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
    },
    {
    "table_name":BAD_SOURCE_TABLE_NAME,
    "query": f"""
            CREATE TABLE IF NOT EXISTS {STAGE_SCHEMA_NAME}.{BAD_SOURCE_TABLE_NAME} (
                error_id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                source TEXT NOT NULL,
                length INTEGER,
                error_dttm TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """
    },
    {
    "table_name":DATA_UPDATE_TABLE_NAME,
    "query": f"""
            CREATE TABLE IF NOT EXISTS {STAGE_SCHEMA_NAME}.{DATA_UPDATE_TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                success boolean,
                message TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """
    },
]
