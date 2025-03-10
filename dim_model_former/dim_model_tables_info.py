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
DIM_MODEL_SCRIPTS = []


######################__CUSTOMERS_TABLE_NAME__#####################

DIM_MODEL_TABLES += [
    {
        "table_name": CUSTOMERS_TABLE_NAME,
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
    }
]

DIM_MODEL_SCRIPTS += [
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
        WITH information_invest_account AS (
            SELECT 
                a.customer_id, 
                SUM(ct.transaction_amt) AS sum_invest_account
            FROM {STAGE_SCHEMA_NAME}.crm_transaction ct 
            JOIN {STAGE_SCHEMA_NAME}.crm_account_type cat 
                ON cat.account_type_cd = ct.transaction_type_cd 
            JOIN {STAGE_SCHEMA_NAME}.crm_account ca 
                ON ca.account_id = ct.account_id 
            JOIN {STAGE_SCHEMA_NAME}."application" a 
                ON ca.application_id = a.application_id 
            WHERE cat.account_type_nm = 'investacc'
            GROUP BY a.customer_id  
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY cc.customer_id) AS customer_dim_id,
            cc.birth_dt AS birth_date,
            cc.first_nm,
            cc.middle_nm,
            cc.last_nm,
            cc.create_dttm,
            DATE_PART('year', AGE(cc.birth_dt)) AS age,
            CASE 
                WHEN iia.sum_invest_account < 100000 THEN 'default' 
                ELSE 'invest' 
            END AS type_customer
        FROM {STAGE_SCHEMA_NAME}.cab_customer cc 
        LEFT JOIN information_invest_account iia 
            ON iia.customer_id = cc.customer_id;
    """
]


######################__TRANSACTION_TABLE_NAME__#####################
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



######################__TRANSACTION_TYPE_TABLE_NAME__#####################

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


######################__CALENDAR_TABLE_NAME__#####################

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





######################__ACCOUNTS_TABLE_NAME__#####################

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

######################__ACTIVATION_DEACTIVATION__#####################

DIM_MODEL_TABLES += [
    {
        "table_name": ACTIVATION_DEACTIVATION,
        "query": f"""
            CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{ACTIVATION_DEACTIVATION} (
                customer_dim_id INTEGER,
                create_dttm_id INTEGER,
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
            create_dttm_id,
            flag_activation,
            activation_id
        )
        WITH activation_deactivation AS (
            SELECT 
                sr.service_request_id, 
                sr.customer_id, 
                sr.create_dttm, 
                CASE 
                    WHEN srt.service_request_type_nm = 'disable' THEN 0 
                    ELSE 1 
                END AS flag_activation
            FROM {STAGE_SCHEMA_NAME}.service_request sr
            JOIN {STAGE_SCHEMA_NAME}.service_request_type srt 
                ON srt.service_request_type_cd = sr.service_request_type_cd
        ),
        activation_deactivation_with_prev AS (
            SELECT *,
                LAG(flag_activation) OVER (
                    PARTITION BY customer_id ORDER BY create_dttm
                ) AS prev_flag_activation
            FROM activation_deactivation
        ),
        filter_data_duplicate AS (
            SELECT *
            FROM activation_deactivation_with_prev
            WHERE flag_activation <> prev_flag_activation 
                OR prev_flag_activation IS NULL
        ),
        filter_data_first_deactivate AS (
            SELECT *
            FROM filter_data_duplicate
            WHERE NOT (prev_flag_activation IS NULL AND flag_activation = 0)
        ),
        customer_dim AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY cc.customer_id) AS customer_dim_id,
                cc.customer_id AS b_customer_id
            FROM {STAGE_SCHEMA_NAME}.cab_customer cc
        ),
        gen_calendar AS (
            SELECT 
                GENERATE_SERIES(
                    DATE '2018-01-01',  -- Начальная дата
                    DATE '2099-12-31',  -- Конечная дата
                    INTERVAL '1 day'    -- Шаг (1 день)
                ) AS calendar_date
        ),
        calendar_row_number AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY calendar_date) AS date_id, 
                calendar_date AS date
            FROM gen_calendar
        )
        SELECT 
            cd.customer_dim_id, 
            crn.date_id AS create_dttm_id, 
            fdf.flag_activation, 
            fdf.service_request_id AS activation_id
        FROM filter_data_first_deactivate fdf
        JOIN customer_dim cd 
            ON fdf.customer_id = cd.b_customer_id
        JOIN calendar_row_number crn 
            ON crn.date = fdf.create_dttm
        ORDER BY fdf.customer_id, fdf.create_dttm;
    """
]



######################__SERVICE_REQUEST_PERIODS_TABLE_NAME__#####################

DIM_MODEL_TABLES += [
    {
        "table_name": SERVICE_REQUEST_PERIODS_TABLE_NAME,
        "query": f"""
            CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{SERVICE_REQUEST_PERIODS_TABLE_NAME} (
                customer_dim_id INTEGER,
                enable_dttm_id INTEGER,
                disable_dttm_id INTEGER,
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
        WITH activation_deactivation AS (
            SELECT 
                sr.service_request_id, 
                sr.customer_id, 
                sr.create_dttm, 
                CASE 
                    WHEN srt.service_request_type_nm = 'disable' THEN 0 
                    ELSE 1 
                END AS flag_activation,
                srt.service_request_type_nm
            FROM {STAGE_SCHEMA_NAME}.service_request sr
            JOIN {STAGE_SCHEMA_NAME}.service_request_type srt 
                ON srt.service_request_type_cd = sr.service_request_type_cd
        ),
        activation_deactivation_with_prev AS (
            SELECT *,
                LAG(flag_activation) OVER (
                    PARTITION BY customer_id ORDER BY create_dttm
                ) AS prev_flag_activation
            FROM activation_deactivation
        ),
        filter_data_duplicate AS (
            SELECT *
            FROM activation_deactivation_with_prev
            WHERE flag_activation <> prev_flag_activation 
                OR prev_flag_activation IS NULL
        ),
        activation_periods AS (
            SELECT
                customer_id,
                create_dttm AS enable_dttm,
                LEAD(create_dttm) OVER (
                    PARTITION BY customer_id ORDER BY create_dttm
                ) AS disable_dttm,
                service_request_id AS service_request_enable_id, 
                LEAD(service_request_id) OVER (
                    PARTITION BY customer_id ORDER BY create_dttm
                ) AS service_request_disable_id,
                service_request_type_nm AS enable_type,
                LEAD(service_request_type_nm) OVER (
                    PARTITION BY customer_id ORDER BY create_dttm
                ) AS disable_type,
                flag_activation
            FROM filter_data_duplicate
        ),
        activation_periods_filter AS (
            SELECT *
            FROM activation_periods
            WHERE flag_activation = 1
        ),
        customer_dim AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY cc.customer_id) AS customer_dim_id,
                cc.customer_id AS b_customer_id
            FROM {STAGE_SCHEMA_NAME}.cab_customer cc
        ),
        gen_calendar AS (
            SELECT 
                GENERATE_SERIES(
                    DATE '2018-01-01',  -- Начальная дата
                    DATE '2099-12-31',  -- Конечная дата
                    INTERVAL '1 day'    -- Шаг (1 день)
                ) AS calendar_date
        ),
        calendar_row_number AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY calendar_date) AS date_id, 
                calendar_date AS date
            FROM gen_calendar
        )
        SELECT 
            cd.customer_dim_id, 
            cr1.date_id AS enable_dttm_id,
            cr2.date_id AS disable_dttm_id,
            apf.service_request_enable_id,
            apf.service_request_disable_id,
            EXTRACT(DAY FROM (
                CASE 
                    WHEN apf.disable_dttm IS NULL 
                    THEN '2021-04-01'::DATE 
                    ELSE apf.disable_dttm 
                END - apf.enable_dttm
            )) AS number_days,
            EXTRACT(MONTH FROM AGE(
                CASE 
                    WHEN apf.disable_dttm IS NULL 
                    THEN '2021-04-01'::DATE 
                    ELSE apf.disable_dttm 
                END, 
                apf.enable_dttm
            )) AS number_months
        FROM activation_periods_filter apf
        JOIN customer_dim cd 
            ON apf.customer_id = cd.b_customer_id
        JOIN calendar_row_number cr1 
            ON cr1.date = apf.enable_dttm
        JOIN calendar_row_number cr2 
            ON cr2.date = (
                CASE 
                    WHEN apf.disable_dttm IS NULL 
                    THEN '2099-12-30'::DATE 
                    ELSE apf.disable_dttm 
                END
            )
        WHERE apf.flag_activation = 1
        ORDER BY cd.customer_dim_id, apf.enable_dttm;
    """
]

