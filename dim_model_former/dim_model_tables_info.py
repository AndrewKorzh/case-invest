import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from config import DIM_MODEL_SCHEMA_NAME, STAGE_SCHEMA_NAME

CUSTOMERS_TABLE_NAME = "customer_dim"
TRANSACTION_TABLE_NAME = "revenue_fact"
TRANSACTION_TYPE_TABLE_NAME = "transaction_type_dim"
CALENDAR_TABLE_NAME = "date_dim"
ACCOUNTS_TABLE_NAME = "account_dim"
ACTIVATION_DEACTIVATION = "activation_deactivation_fact"
SERVICE_REQUEST_PERIODS_TABLE_NAME = "retention_fact"

DIM_MODEL_TABLES = []


# CUSTOMERS_TABLE_NAME
DIM_MODEL_TABLES += [
    {
    "table_name":CUSTOMERS_TABLE_NAME,
    "query": f"""
        CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{CUSTOMERS_TABLE_NAME} (
            customer_dim_id INTEGER,
            birth_dt DATE,
            first_nm VARCHAR(255),
            middle_nm VARCHAR(255),
            last_nm VARCHAR(255),
            create_dttm TIMESTAMP,
            age INTEGER,
            type_customer VARCHAR(20) DEFAULT 'default'
        );
        """
    },
]

DIM_MODEL_SCRIPTS = [
f"""
    INSERT INTO {DIM_MODEL_SCHEMA_NAME}.{CUSTOMERS_TABLE_NAME} (
        customer_dim_id,
        birth_dt,
        first_nm,
        middle_nm,
        last_nm,
        create_dttm,
        age,
        type_customer
    )
    SELECT 
        row_number() over (order by cc.customer_id) as customer_dim_id,
        cc.birth_dt as birth_date,
        cc.first_nm,
        cc.middle_nm,
        cc.last_nm,
        cc.create_dttm,
        date_part('year', age(cc.birth_dt)) as age,
        'default' as type_customer
    FROM {STAGE_SCHEMA_NAME}.cab_customer cc;
"""
]


# TRANSACTION_TABLE_NAME
DIM_MODEL_TABLES += [
    {
        "table_name": TRANSACTION_TABLE_NAME,
        "query": f"""
            CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{TRANSACTION_TABLE_NAME} (
                id_transaction INTEGER,
                customer_dim_id INTEGER,
                account_dim_id INTEGER,
                transaction_dttm TIMESTAMP,
                transaction_type_dim_id INTEGER,
                commision_ammount NUMERIC(10,2),
                lost_profit_ammount NUMERIC(10,2) DEFAULT 0
            );
        """
    }
]

DIM_MODEL_SCRIPTS += [
    f"""
        INSERT INTO {DIM_MODEL_SCHEMA_NAME}.{TRANSACTION_TABLE_NAME} (
            id_transaction,
            customer_dim_id,
            account_dim_id,
            transaction_dttm,
            transaction_type_dim_id,
            commision_ammount,
            lost_profit_ammount
        )
        WITH account_dim AS (
            SELECT 
                row_number() OVER (ORDER BY ca.account_id) AS account_dim_id,
                ca.account_id AS b_account_id,
                ca.application_id
            FROM {STAGE_SCHEMA_NAME}.crm_account ca
        ),
        customer_dim AS (
            SELECT 
                row_number() OVER (ORDER BY cc.customer_id) AS customer_dim_id,
                cc.customer_id AS b_customer_id
            FROM {STAGE_SCHEMA_NAME}.cab_customer cc
        ),
        customer_dim_with_appeal AS (
            SELECT 
                cdd.customer_dim_id, 
                cdd.b_customer_id, 
                app.application_id
            FROM customer_dim cdd
            JOIN {STAGE_SCHEMA_NAME}."application" app 
                ON app.customer_id = cdd.b_customer_id
        ),
        customer_account_dim AS (
            SELECT 
                ad.account_dim_id, 
                cda.customer_dim_id, 
                ad.b_account_id
            FROM customer_dim_with_appeal cda
            JOIN account_dim ad 
                ON ad.application_id = cda.application_id
        )
        SELECT 
            ct.transaction_id AS id_transaction, 
            cad.customer_dim_id, 
            cad.account_dim_id, 
            ct.transaction_dttm, 
            ct.transaction_type_cd AS transaction_type_dim_id, 
            ct.transaction_amt * 0.01 AS commision_ammount, 
            0 AS lost_profit_ammount
        FROM {STAGE_SCHEMA_NAME}.crm_transaction ct
        JOIN {STAGE_SCHEMA_NAME}.crm_transaction_type ctt 
            ON ct.transaction_type_cd = ctt.transaction_type_cd
        JOIN customer_account_dim cad 
            ON cad.b_account_id = ct.account_id
        WHERE ctt.transaction_type_nm = 'kopylka' 
            AND ct.transaction_amt > 0;
    """
]

DIM_MODEL_TABLES += [
    {
        "table_name": TRANSACTION_TYPE_TABLE_NAME,
        "query": f"""
            CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{TRANSACTION_TYPE_TABLE_NAME} (
                transaction_type_dim_id INTEGER PRIMARY KEY,
                transaction_type_nm VARCHAR(255)
            );
        """
    }
]

DIM_MODEL_SCRIPTS += [
    f"""
        INSERT INTO {DIM_MODEL_SCHEMA_NAME}.{TRANSACTION_TYPE_TABLE_NAME} (
            transaction_type_dim_id,
            transaction_type_nm
        )
        SELECT 
            transaction_type_cd AS transaction_type_dim_id,
            transaction_type_nm
        FROM {STAGE_SCHEMA_NAME}.crm_transaction_type;
    """
]

# CALENDAR_TABLE_NAME
DIM_MODEL_TABLES += [
    {
        "table_name": CALENDAR_TABLE_NAME,
        "query": f"""
            CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{CALENDAR_TABLE_NAME} (
                date_id INTEGER PRIMARY KEY,
                date DATE,
                day_date INTEGER,
                day_of_week INTEGER,
                month INTEGER,
                year INTEGER
            );
        """
    }
]


DIM_MODEL_SCRIPTS += [
    f"""
        INSERT INTO {DIM_MODEL_SCHEMA_NAME}.{CALENDAR_TABLE_NAME} (
            date_id,
            date,
            day_date,
            day_of_week,
            month,
            year
        )
        WITH gen_calendar AS (
            SELECT 
                generate_series(
                    DATE '2018-01-01',  -- Начальная дата
                    DATE '2099-12-31',  -- Конечная дата
                    INTERVAL '1 day'    -- Шаг (1 день)
                ) AS calendar_date
        )
        SELECT 
            row_number() OVER (ORDER BY calendar_date) AS date_id, 
            calendar_date AS date,
            EXTRACT(DAY FROM calendar_date) AS day_date,
            EXTRACT(DOW FROM calendar_date) AS day_of_week,
            EXTRACT(MONTH FROM calendar_date) AS month,
            EXTRACT(YEAR FROM calendar_date) AS year
        FROM gen_calendar;
    """
]



DIM_MODEL_TABLES += [
    {
        "table_name": ACCOUNTS_TABLE_NAME,
        "query": f"""
            CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{ACCOUNTS_TABLE_NAME} (
                account_dim_id INTEGER,
                account_create_date DATE,
                account_type VARCHAR(255),
                account_status_cd VARCHAR(255),
                account_id INTEGER
            );
        """
    }
]

DIM_MODEL_SCRIPTS += [
    f"""
        INSERT INTO {DIM_MODEL_SCHEMA_NAME}.{ACCOUNTS_TABLE_NAME} (
            account_dim_id,
            account_create_date,
            account_type,
            account_status_cd,
            account_id
        )
        SELECT 
            row_number() OVER (ORDER BY ca.account_id) AS account_dim_id,
            ca.acccount_create_dt AS account_create_date,
            cat.account_type_nm AS account_type,
            cas.account_status_nm AS account_status_cd,
            ca.account_id AS account_id
        FROM {STAGE_SCHEMA_NAME}.crm_account ca
        JOIN {STAGE_SCHEMA_NAME}.crm_account_status cas 
            ON ca.account_status_cd = cas.account_status_cd
        JOIN {STAGE_SCHEMA_NAME}.crm_account_type cat 
            ON ca.account_type_cd = cat.account_type_cd;
    """
]


DIM_MODEL_TABLES += [
    {
        "table_name": ACTIVATION_DEACTIVATION,
        "query": f"""
            CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{ACTIVATION_DEACTIVATION} (
                customer_dim_id INTEGER,
                create_dttm TIMESTAMP,
                flag_activation INTEGER,
                activation_id INTEGER
            );
        """
    }
]

DIM_MODEL_SCRIPTS += [
    f"""
        INSERT INTO {DIM_MODEL_SCHEMA_NAME}.{ACTIVATION_DEACTIVATION} (
            customer_dim_id,
            create_dttm,
            flag_activation,
            activation_id
        )
        WITH service_request_activate_deactivate AS (
            SELECT  
                sr.service_request_id, 
                sr.customer_id, 
                sr.create_dttm, 
                CASE 
                    WHEN srt.service_request_type_nm = 'enable' THEN 1 
                    WHEN srt.service_request_type_nm = 'disable' THEN 0 
                END AS flag_activation
            FROM {STAGE_SCHEMA_NAME}.service_request sr
            JOIN {STAGE_SCHEMA_NAME}.service_request_type srt 
                ON srt.service_request_type_cd = sr.service_request_type_cd 
            WHERE srt.service_request_type_nm IN ('enable', 'disable')
        ),
        service_request_with_prev_activate_flag AS (
            SELECT *,
                LAG(flag_activation) OVER (
                    PARTITION BY customer_id ORDER BY create_dttm
                ) AS prev_flag_activation
            FROM service_request_activate_deactivate
        ),
        service_request_filter_data AS (
            SELECT
                service_request_id,
                customer_id,
                create_dttm,
                flag_activation
            FROM service_request_with_prev_activate_flag
            WHERE flag_activation <> prev_flag_activation 
                OR (prev_flag_activation IS NULL AND flag_activation = 1) 
                OR NOT (prev_flag_activation IS NULL AND flag_activation = 0)
        ),
        customer_dim AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY cc.customer_id) AS customer_dim_id,
                cc.customer_id AS b_customer_id
            FROM {STAGE_SCHEMA_NAME}.cab_customer cc
        )
        SELECT 
            cd.customer_dim_id, 
            srf.create_dttm, 
            srf.flag_activation, 
            srf.service_request_id AS activation_id
        FROM service_request_filter_data srf
        JOIN customer_dim cd 
            ON srf.customer_id = cd.b_customer_id
        ORDER BY srf.customer_id, srf.create_dttm;
    """
]



DIM_MODEL_TABLES += [
    {
        "table_name": SERVICE_REQUEST_PERIODS_TABLE_NAME,
        "query": f"""
            CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{SERVICE_REQUEST_PERIODS_TABLE_NAME} (
                customer_dim_id INTEGER,
                enable_dttm_id TIMESTAMP,
                disable_dttm_id TIMESTAMP,
                service_request_enable_id INTEGER,
                service_request_disable_id INTEGER,
                number_days INTEGER,
                number_months INTEGER
            );
        """
    }
]

DIM_MODEL_SCRIPTS += [
    f"""
        INSERT INTO {DIM_MODEL_SCHEMA_NAME}.{SERVICE_REQUEST_PERIODS_TABLE_NAME} (
            customer_dim_id,
            enable_dttm_id,
            disable_dttm_id,
            service_request_enable_id,
            service_request_disable_id,
            number_days,
            number_months
        )
        WITH service_request_with_type AS (
            SELECT 
                sr.service_request_id, 
                sr.customer_id, 
                srt.service_request_type_nm, 
                sr.service_request_status_cd, 
                sr.create_dttm 
            FROM {STAGE_SCHEMA_NAME}.service_request sr 
            JOIN {STAGE_SCHEMA_NAME}.service_request_type srt 
                ON sr.service_request_type_cd = srt.service_request_type_cd
        ),
        service_request_with_type_activate_deactivate AS (
            SELECT *
            FROM service_request_with_type
            WHERE service_request_type_nm IN ('enable', 'disable')
        ),
        service_request_with_prev_activate_flag AS (
            SELECT *, 
                LAG(service_request_type_nm) OVER (
                    PARTITION BY customer_id ORDER BY create_dttm
                ) AS prev_type
            FROM service_request_with_type_activate_deactivate
        ),
        service_request_filtered_data AS (
            SELECT 
                service_request_id, 
                customer_id, 
                service_request_type_nm, 
                service_request_status_cd, 
                create_dttm 
            FROM service_request_with_prev_activate_flag
            WHERE service_request_type_nm <> prev_type OR prev_type IS NULL
        ),
        service_request_periods_lead AS (
            SELECT
                customer_id,
                create_dttm AS enable_dttm_id,
                LEAD(create_dttm) OVER (
                    PARTITION BY customer_id ORDER BY create_dttm
                ) AS disable_dttm_id,
                service_request_id AS service_request_enable_id, 
                LEAD(service_request_id) OVER (
                    PARTITION BY customer_id ORDER BY create_dttm
                ) AS service_request_disable_id,
                service_request_type_nm AS enable_type,
                LEAD(service_request_type_nm) OVER (
                    PARTITION BY customer_id ORDER BY create_dttm
                ) AS disable_type
            FROM service_request_filtered_data
        ),
        customer_dim AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY cc.customer_id) AS customer_dim_id,
                cc.customer_id AS b_customer_id
            FROM {STAGE_SCHEMA_NAME}.cab_customer cc
        )
        SELECT 
            cd.customer_dim_id, 
            spr.enable_dttm_id,
            spr.disable_dttm_id,
            spr.service_request_enable_id,
            spr.service_request_disable_id,
            EXTRACT(DAY FROM (
                CASE 
                    WHEN spr.disable_dttm_id IS NULL 
                    THEN '2021-04-01'::DATE 
                    ELSE spr.disable_dttm_id 
                END - spr.enable_dttm_id
            )) AS number_days,
            EXTRACT(MONTH FROM AGE(
                CASE 
                    WHEN spr.disable_dttm_id IS NULL 
                    THEN '2021-04-01'::DATE 
                    ELSE spr.disable_dttm_id 
                END, 
                spr.enable_dttm_id
            )) AS number_months
        FROM service_request_periods_lead spr
        JOIN customer_dim cd 
            ON spr.customer_id = cd.b_customer_id
        WHERE spr.enable_type = 'enable'
        ORDER BY cd.customer_dim_id, spr.enable_dttm_id;
    """
]
