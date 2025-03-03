from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from config import STAGE_SCHEMA_NAME


TABLES = [
f"""
create table {STAGE_SCHEMA_NAME}.CRM_TRANSACTION_TYPE
(
  transaction_type_cd int primary key,
  transaction_type_nm text
);
""",
f"""
create table {STAGE_SCHEMA_NAME}.CRM_ACCOUNT_TYPE
(
  account_type_cd int primary key,
  account_type_nm text
);

""",

f"""
create table {STAGE_SCHEMA_NAME}.CRM_ACCOUNT_STATUS
(
  account_status_cd int primary key,
  account_status_nm text
);
""",

f"""
create table {STAGE_SCHEMA_NAME}.SERVICE_REQUEST_TYPE
(
  service_request_type_cd int primary key,
  service_request_type_nm text
);
""",

f"""
create table {STAGE_SCHEMA_NAME}.SERVICE_REQUEST_STATUS
(
  service_request_status_cd int primary key,
  service_request_status_nm text
);
""",

f"""
create table {STAGE_SCHEMA_NAME}.CRM_TRANSACTION
(
  transaction_id int primary key,
  orig_id int,  
  account_id int,
  transaction_type_cd int,
  transaction_amt decimal(12,6),
  transaction_dttm timestamp,
  create_dttm timestamp,
  delete_dttm timestamp
);""",

f"""
create table {STAGE_SCHEMA_NAME}.CRM_ACCOUNT
(
  account_id int primary key,
  account_type_cd int,
  acccount_create_dt date,
  account_status_cd int,
  application_id int,
  create_dttm timestamp,
  delete_dttm timestamp
);""",

f"""
create table {STAGE_SCHEMA_NAME}.PRODUCT_TYPE
(
  product_type_cd int primary key,
  product_type_nm text
);""",

f"""
create table {STAGE_SCHEMA_NAME}.APPLICATION
(
  application_id int primary key,
  product_type_cd int,
  customer_id int,
  advert_source_id int,
  create_dttm timestamp,
  delete_dttm timestamp
);""",


f"""
create table {STAGE_SCHEMA_NAME}.CRM_CUSTOMER
(
    crm_customer_id int primary key,
    customer_id int,
    birth_dt timestamp,
    phone_num text,
    email text,
    first_nm text,
    last_nm text,
    create_dttm timestamp,
    delete_dttm timestamp
);""",

f"""
create table {STAGE_SCHEMA_NAME}.SERVICE_REQUEST
(
  service_request_id int primary key,
  customer_id int,
  service_request_type_cd int,
  service_request_status_cd int,
  tail_limit int,
  create_dttm timestamp,
  delete_dttm timestamp
);""",

f"""
create table {STAGE_SCHEMA_NAME}.ADVERT_SOURCE
(
  advert_source_id int primary key,
  advert_source_nm text,
  monthly_payment_amt decimal(12,6),
  start_month int,
  end_month int,
  create_dttm timestamp,
  delete_dttm timestamp
);""",

f"""
create table {STAGE_SCHEMA_NAME}.CAB_CUSTOMER
(
    customer_id int primary key,
    birth_dt date,
    passport_num text,
    phone_num text,
    add_phone_num text,
    email text,
    reg_address_txt text,
    fact_address_txt text,
    first_nm text,
    last_nm text,
    middle_nm text,
    create_dttm timestamp,
    delete_dttm timestamp
);""",

f"""
CREATE TABLE {STAGE_SCHEMA_NAME}.error_log (
    id SERIAL PRIMARY KEY,
    operation_type VARCHAR(50) NOT NULL,
    error_message TEXT NOT NULL,
    row_data JSONB NOT NULL
);
"""
]

FOREIGN_KEYS = [

f"""
alter table {STAGE_SCHEMA_NAME}.SERVICE_REQUEST
add foreign key (service_request_type_cd)
references {STAGE_SCHEMA_NAME}.SERVICE_REQUEST_TYPE(service_request_type_cd)
""",

f"""
alter table {STAGE_SCHEMA_NAME}.SERVICE_REQUEST
add foreign key (service_request_status_cd)
references {STAGE_SCHEMA_NAME}.SERVICE_REQUEST_STATUS(service_request_status_cd)
""",

f"""
alter table {STAGE_SCHEMA_NAME}.SERVICE_REQUEST
add foreign key (customer_id)
references {STAGE_SCHEMA_NAME}.CAB_CUSTOMER(customer_id)
""",

f"""
alter table {STAGE_SCHEMA_NAME}.CRM_CUSTOMER
add foreign key (customer_id)
references {STAGE_SCHEMA_NAME}.CAB_CUSTOMER(customer_id)
""",

f"""
alter table {STAGE_SCHEMA_NAME}.APPLICATION
add foreign key (customer_id)
references {STAGE_SCHEMA_NAME}.CAB_CUSTOMER(customer_id)
""",

f"""
alter table {STAGE_SCHEMA_NAME}.APPLICATION
add foreign key (advert_source_id)
references {STAGE_SCHEMA_NAME}.ADVERT_SOURCE(advert_source_id)
""",

f"""
alter table {STAGE_SCHEMA_NAME}.APPLICATION
add foreign key (product_type_cd)
references {STAGE_SCHEMA_NAME}.PRODUCT_TYPE(product_type_cd)
""",

f"""
alter table {STAGE_SCHEMA_NAME}.CRM_ACCOUNT
add foreign key (application_id)
references {STAGE_SCHEMA_NAME}.APPLICATION(application_id)
""",

f"""
alter table {STAGE_SCHEMA_NAME}.CRM_ACCOUNT
add foreign key (account_type_cd)
references {STAGE_SCHEMA_NAME}.CRM_ACCOUNT_TYPE(account_type_cd)
""",

f"""
alter table {STAGE_SCHEMA_NAME}.CRM_ACCOUNT
add foreign key (account_status_cd)
references {STAGE_SCHEMA_NAME}.CRM_ACCOUNT_STATUS(account_status_cd)
""",

f"""
alter table {STAGE_SCHEMA_NAME}.CRM_TRANSACTION
add foreign key (account_id)
references {STAGE_SCHEMA_NAME}.CRM_ACCOUNT(account_id)
""",

f"""
alter table {STAGE_SCHEMA_NAME}.CRM_TRANSACTION
add foreign key (transaction_type_cd)
references {STAGE_SCHEMA_NAME}.CRM_TRANSACTION_TYPE(transaction_type_cd)
""",

]


TRIGGERS = [
    
    f"""
    CREATE OR REPLACE FUNCTION log_duplicate_key()
    RETURNS TRIGGER AS $$
    BEGIN
        -- Проверка уникальности
        IF EXISTS (SELECT 1 FROM {STAGE_SCHEMA_NAME}.crm_transaction WHERE transaction_id = NEW.transaction_id) THEN
            -- Запись ошибки в error_log
            INSERT INTO {STAGE_SCHEMA_NAME}.error_log (operation_type, error_message, row_data)
            VALUES (
                'INSERT',
                format('Duplicate transaction_id: %s', row_to_json(NEW)),
                row_to_json(NEW)
            );
            -- Пропуск записи
            RETURN NULL;
        END IF;

        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """,
    
    f"""
    -- Привязка триггера
    CREATE TRIGGER log_duplicate_key_trigger
    BEFORE INSERT ON {STAGE_SCHEMA_NAME}.crm_transaction
    FOR EACH ROW
    EXECUTE FUNCTION log_duplicate_key();
    """
]