import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from config import DIM_MODEL_SCHEMA_NAME, STAGE_SCHEMA_NAME

CUSTOMERS_DIM = "customer_dim"
REVENUE_FACT = "revenue_fact"
TRANSACTION_TYPE_DIM = "transaction_type_dim"
DATE_DIM = "date_dim"
ACCOUNT_DIM = "account_dim"
ACTIVATION_DEACTIVATION_FACT = "activation_deactivation_fact"
RETENTION_FACT = "retention_fact"
LAST_DATA_UPDATE_DATE = "last_data_update_date"

DIM_MODEL_TABLES = {}
DIM_MODEL_SCRIPTS = {}




###################### CUSTOMERS_DIM #####################

DIM_MODEL_TABLES[CUSTOMERS_DIM] = f"""
        CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{CUSTOMERS_DIM} (
            customer_dim_id INTEGER,
            birth_date DATE,
            first_nm VARCHAR(255),
            middle_nm VARCHAR(255),
            last_nm VARCHAR(255),
            create_dttm TIMESTAMP,
            age INTEGER,
            type_customer VARCHAR(20),
            invest_strateg VARCHAR(20),
            source_customer_id INTEGER
        );
        """
    

DIM_MODEL_SCRIPTS[CUSTOMERS_DIM] = f"""
    INSERT INTO {DIM_MODEL_SCHEMA_NAME}.{CUSTOMERS_DIM} (
        customer_dim_id,
        birth_date,
        first_nm,
        middle_nm,
        last_nm,
        create_dttm,
        age,
        type_customer,
        invest_strateg,
        source_customer_id
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
    ),
    avg_invest AS (
        SELECT AVG(sum_invest_account) AS avg
        FROM information_invest_account
    ),
    data AS (
        SELECT 
            sr.customer_id,
            AVG(sr.tail_limit) AS avg_tail_limit
        FROM {STAGE_SCHEMA_NAME}.service_request sr
        JOIN {STAGE_SCHEMA_NAME}.service_request_type srt 
            ON srt.service_request_type_cd = sr.service_request_type_cd
        WHERE srt.service_request_type_nm <> 'disable'
        GROUP BY sr.customer_id
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
            WHEN iia.sum_invest_account < (SELECT avg FROM avg_invest) THEN 'default'
            ELSE 'invest'
        END AS type_customer,
        CASE 
            WHEN fd.avg_tail_limit < 100 THEN 'conservative'
            WHEN fd.avg_tail_limit < 500 THEN 'balanced'
            WHEN fd.avg_tail_limit > 500 THEN 'aggressive'
            ELSE NULL
        END AS invest_strateg,
        cc.customer_id AS source_customer_id
    FROM {STAGE_SCHEMA_NAME}.cab_customer cc
    JOIN information_invest_account iia 
        ON iia.customer_id = cc.customer_id
    LEFT JOIN data fd 
        ON fd.customer_id = cc.customer_id;
    """


###################### REVENUE_FACT #####################
DIM_MODEL_TABLES[REVENUE_FACT] = f"""
        CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{REVENUE_FACT} (
            revenue_id INTEGER,
            transaction_id INTEGER,
            customer_dim_id INTEGER,
            account_dim_id INTEGER,
            transaction_dttm_id INTEGER,
            transaction_type_dim_id INTEGER,
            commision_ammount NUMERIC(10, 2),
            lost_profit_ammount NUMERIC(10, 2)
        );
        """
    

DIM_MODEL_SCRIPTS[REVENUE_FACT] = f"""
INSERT INTO {DIM_MODEL_SCHEMA_NAME}.{REVENUE_FACT} (
    revenue_id,
    transaction_id,
    customer_dim_id,
    account_dim_id,
    transaction_dttm_id,
    transaction_type_dim_id,
    commision_ammount,
    lost_profit_ammount
) select 
	row_number() over (order by 1) as revenue_id, 
	*
from (
(with account_dim as (
	select row_number() over (order by ca.account_id) as account_dim_id,
	ca.account_id as b_account_id,
	ca.application_id
	from {STAGE_SCHEMA_NAME}.crm_account ca 
),
customer_dim as (
	select row_number() over (order by cc.customer_id) as customer_dim_id,
	cc.customer_id as b_customer_id
	from {STAGE_SCHEMA_NAME}.cab_customer cc 
),
customer_dim_with_appeal as (
	select customer_dim_id, b_customer_id, app.application_id
	from customer_dim cdd
	join {STAGE_SCHEMA_NAME}."application" app on app.customer_id = cdd.b_customer_id
),
customer_account_dim as (
	select account_dim_id, customer_dim_id, b_account_id
	from customer_dim_with_appeal cda
	join account_dim ad on ad.application_id = cda.application_id
),
gen_calendar as (select 
    generate_series(
        DATE '2018-01-01',  -- Начальная дата
        DATE '2099-12-31',  -- Конечная дата
        INTERVAL '1 day'    -- Шаг (1 день)
    ) as calendar_date
 ),
 calendar_row_number as (
	select 
	row_number() over (order by calendar_date) as date_id, 
 	calendar_date as date
 	from gen_calendar
)
select 
ct.transaction_id as transaction_id, 
cad.customer_dim_id, 
cad.account_dim_id, 
date_id as transaction_dttm_id, 
ct.transaction_type_cd as transaction_type_dim_id, 
abs(ct.transaction_amt) * 0.01 as commision_ammount, 
0 as lost_profit_ammount
from {STAGE_SCHEMA_NAME}.crm_transaction ct 
join {STAGE_SCHEMA_NAME}.crm_transaction_type ctt on ct.transaction_type_cd = ctt.transaction_type_cd
join customer_account_dim cad on cad.b_account_id = ct.account_id 
join calendar_row_number on date = ct.transaction_dttm
where ctt.transaction_type_nm = 'kopylka' and ct.transaction_amt < 0)
union 
(with avg_tail_limit as 
	(select 
	round(avg(sr.tail_limit)) as avg_tl
	from {STAGE_SCHEMA_NAME}.service_request sr 
	join {STAGE_SCHEMA_NAME}.crm_transaction_type ctt on ctt.transaction_type_cd = sr.service_request_type_cd 
	where ctt.transaction_type_nm <> 'disable'),
data_with_row_nm as (
	select 
	sr.customer_id,
	sr.tail_limit,
	sr.create_dttm,
	row_number() over (partition by sr.customer_id order by sr.create_dttm desc) as rw
	from {STAGE_SCHEMA_NAME}.service_request sr
	join {STAGE_SCHEMA_NAME}.service_request_type srt on srt.service_request_type_cd = sr.service_request_type_cd 
	where srt.service_request_type_nm <> 'disable'
),
filter_data as (
	select *
	from data_with_row_nm
	where rw = 1
),
customer_tail_limit_with_avg as (select 
	cc.customer_id,
	fd.tail_limit,
	(select * from avg_tail_limit) as avg_tl
	from {STAGE_SCHEMA_NAME}.cab_customer cc
	left join filter_data fd on cc.customer_id = fd.customer_id
),
customer_tail_limit as (
	select customer_id, coalesce(tail_limit, avg_tl) as tail_limit
	from customer_tail_limit_with_avg
),
account_tail_limit as (
	select ct.customer_id,
	ca.account_id,
	tail_limit
	from {STAGE_SCHEMA_NAME}.crm_account ca 
	join {STAGE_SCHEMA_NAME}."application" a on a.application_id = ca.application_id
	join customer_tail_limit ct on a.customer_id = ct.customer_id
),
transactions_with_count_orig_id as (
	select *,
	count(orig_id) over (partition by orig_id) as count_orig_id
	from {STAGE_SCHEMA_NAME}.crm_transaction ct
),
filter_transactions as (
	select *
	from transactions_with_count_orig_id
	where count_orig_id = 1
), 
transactions_with_tail_limit as (
	select ft.*, tail_limit
	from filter_transactions ft
	join account_tail_limit atl on atl.account_id = ft.account_id
),
account_dim as (
	select row_number() over (order by ca.account_id) as account_dim_id,
	ca.account_id as b_account_id,
	ca.application_id
	from {STAGE_SCHEMA_NAME}.crm_account ca 
),
customer_dim as (
	select row_number() over (order by cc.customer_id) as customer_dim_id,
	cc.customer_id as b_customer_id
	from {STAGE_SCHEMA_NAME}.cab_customer cc 
),
customer_dim_with_appeal as (
	select customer_dim_id, b_customer_id, app.application_id
	from customer_dim cdd
	join {STAGE_SCHEMA_NAME}."application" app on app.customer_id = cdd.b_customer_id
),
customer_account_dim as (
	select account_dim_id, customer_dim_id, b_account_id
	from customer_dim_with_appeal cda
	join account_dim ad on ad.application_id = cda.application_id
),
gen_calendar as (select 
    generate_series(
        DATE '2018-01-01',  -- Начальная дата
        DATE '2099-12-31',  -- Конечная дата
        INTERVAL '1 day'    -- Шаг (1 день)
    ) as calendar_date
 ),
 calendar_row_number as (
	select 
	row_number() over (order by calendar_date) as date_id, 
 	calendar_date as date
 	from gen_calendar
)
select 
f.transaction_id as transaction_id, 
cad.customer_dim_id, 
cad.account_dim_id, 
date_id as transaction_dttm_id, 
f.transaction_type_cd as transaction_type_dim_id, 
0 as commision_ammount, 
(tail_limit - (transaction_amt % tail_limit)) * 0.01 as lost_profit_ammount
from transactions_with_tail_limit f
join calendar_row_number on date = f.transaction_dttm
join customer_account_dim cad on cad.b_account_id = f.account_id)) as t

"""
# logger.log(DIM_MODEL_SCRIPTS[len(DIM_MODEL_SCRIPTS)-1])

######################  TRANSACTION_TYPE_DIM  #####################

DIM_MODEL_TABLES[TRANSACTION_TYPE_DIM] = f"""
            CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{TRANSACTION_TYPE_DIM} (
                transaction_type_dim_id INTEGER PRIMARY KEY,
                transaction_type_nm VARCHAR(255)
            );
        """
    


DIM_MODEL_SCRIPTS[TRANSACTION_TYPE_DIM] = f"""
        INSERT INTO {DIM_MODEL_SCHEMA_NAME}.{TRANSACTION_TYPE_DIM} (
            transaction_type_dim_id,
            transaction_type_nm
        )
        SELECT 
            transaction_type_cd AS transaction_type_dim_id,
            transaction_type_nm
        FROM {STAGE_SCHEMA_NAME}.crm_transaction_type;
    """


###################### DATE_DIM #####################

DIM_MODEL_TABLES[DATE_DIM] = f"""
        CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{DATE_DIM} (
            date_id INTEGER,
            calendar_date DATE,
            day_of_month INTEGER,
            day_of_week INTEGER,
            month INTEGER,
            year INTEGER
        );
        """
    

DIM_MODEL_SCRIPTS[DATE_DIM] = f"""
    INSERT INTO {DIM_MODEL_SCHEMA_NAME}.{DATE_DIM} (
        date_id,
        calendar_date,
        day_of_month,
        day_of_week,
        month,
        year
    )
    WITH gen_calendar AS (
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
        date_id,
        date AS calendar_date,
        EXTRACT('day' FROM date) AS day_of_month,
        EXTRACT('dow' FROM date) AS day_of_week,
        EXTRACT('month' FROM date) AS month,
        EXTRACT('year' FROM date) AS year
    FROM calendar_row_number;
    """

######################__ACCOUNTS_TABLE_NAME__#####################

DIM_MODEL_TABLES[ACCOUNT_DIM] = f"""
        CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{ACCOUNT_DIM} (
            account_dim_id INTEGER,
            acccount_create_date DATE,
            account_type VARCHAR(255),
            account_id INTEGER
        );
        """
    

DIM_MODEL_SCRIPTS[ACCOUNT_DIM] = f"""
    INSERT INTO {DIM_MODEL_SCHEMA_NAME}.{ACCOUNT_DIM} (
        account_dim_id,
        acccount_create_date,
        account_type,
        account_id
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY ca.account_id) AS account_dim_id,
        ca.acccount_create_dt AS acccount_create_date,
        cat.account_type_nm AS account_type,
        ca.account_id AS account_id
    FROM {STAGE_SCHEMA_NAME}.crm_account ca
    JOIN {STAGE_SCHEMA_NAME}.crm_account_status cas 
        ON ca.account_status_cd = cas.account_status_cd
    JOIN {STAGE_SCHEMA_NAME}.crm_account_type cat 
        ON ca.account_type_cd = cat.account_type_cd;
    """

######################__ACTIVATION_DEACTIVATION__#####################

DIM_MODEL_TABLES[ACTIVATION_DEACTIVATION_FACT] = f"""
        CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{ACTIVATION_DEACTIVATION_FACT} (
            activation_deactivation_id INTEGER,
            customer_dim_id INTEGER,
            create_dttm_id INTEGER,
            flag_activation INTEGER,
            service_request_id INTEGER
        );
        """
    
DIM_MODEL_SCRIPTS[ACTIVATION_DEACTIVATION_FACT] = f"""
    INSERT INTO {DIM_MODEL_SCHEMA_NAME}.{ACTIVATION_DEACTIVATION_FACT} (
        activation_deactivation_id,
        customer_dim_id,
        create_dttm_id,
        flag_activation,
        service_request_id
    )
    WITH acitvation_deactivation AS (
        SELECT 
            sr.service_request_id, 
            sr.customer_id, 
            sr.create_dttm, 
            CASE WHEN srt.service_request_type_nm = 'disable' THEN 0 ELSE 1 END AS flag_activation
        FROM {STAGE_SCHEMA_NAME}.service_request sr
        JOIN {STAGE_SCHEMA_NAME}.service_request_type srt 
            ON srt.service_request_type_cd = sr.service_request_type_cd
    ),
    acitvation_deactivation_with_prev AS (
        SELECT *,
            LAG(flag_activation) OVER (PARTITION BY customer_id ORDER BY create_dttm) AS prev_flag_activation
        FROM acitvation_deactivation
    ),
    filter_data_duplicate AS (
        SELECT *
        FROM acitvation_deactivation_with_prev
        WHERE flag_activation <> prev_flag_activation OR prev_flag_activation IS NULL
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
        ROW_NUMBER() OVER (ORDER BY service_request_id) AS activation_deactivation_id,
        cd.customer_dim_id, 
        crn.date_id AS create_dttm_id, 
        fdf.flag_activation, 
        fdf.service_request_id
    FROM filter_data_first_deactivate fdf
    JOIN customer_dim cd ON fdf.customer_id = cd.b_customer_id
    JOIN calendar_row_number crn ON crn.date = fdf.create_dttm
    ORDER BY fdf.customer_id, fdf.create_dttm;
    """

######################__SERVICE_REQUEST_PERIODS_TABLE_NAME__#####################

DIM_MODEL_TABLES[RETENTION_FACT] = f"""
        CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{RETENTION_FACT} (
            retantion_id INTEGER,
            customer_dim_id INTEGER,
            enable_dttm_id INTEGER,
            disable_dttm_id INTEGER,
            service_request_enable_id INTEGER,
            service_request_disable_id INTEGER,
            number_days INTEGER,
            number_months INTEGER
        );
        """
    

DIM_MODEL_SCRIPTS[RETENTION_FACT] =  f"""
    INSERT INTO {DIM_MODEL_SCHEMA_NAME}.{RETENTION_FACT} (
        retantion_id,
        customer_dim_id,
        enable_dttm_id,
        disable_dttm_id,
        service_request_enable_id,
        service_request_disable_id,
        number_days,
        number_months
    )
    with acitvation_deactivation as (
	select 
	sr.service_request_id, 
	sr.customer_id, 
	sr.create_dttm, 
	(case when srt.service_request_type_nm = 'disable' then 0 else 1 end) as flag_activation,
	srt.service_request_type_nm
	from {STAGE_SCHEMA_NAME}.service_request sr
	join {STAGE_SCHEMA_NAME}.service_request_type srt on srt.service_request_type_cd = sr.service_request_type_cd),
acitvation_deactivation_with_prev as (select *,
	lag(flag_activation) over (partition by customer_id order by create_dttm) as prev_flag_activation
	from acitvation_deactivation
),
filter_data_duplicate as (
	select *
	from acitvation_deactivation_with_prev
	where flag_activation <> prev_flag_activation or prev_flag_activation is null
),
activation_preiods as (select
	customer_id,
	create_dttm as enable_dttm,
	lead(create_dttm) over (partition by customer_id order by create_dttm) as disable_dttm,
	service_request_id as service_request_enable_id, 
	lead(service_request_id) over (partition by customer_id order by create_dttm) as service_request_disable_id,
	service_request_type_nm as enable_type,
	lead(service_request_type_nm) over (partition  by customer_id order by create_dttm) as disable_type,
	flag_activation
	from filter_data_duplicate
),
activation_preiods_filter as (select *
	from activation_preiods
	where flag_activation = 1
),
customer_dim as (
	select row_number() over (order by cc.customer_id) as customer_dim_id,
	cc.customer_id as b_customer_id
	from {STAGE_SCHEMA_NAME}.cab_customer cc 
),
gen_calendar as (select 
    generate_series(
        DATE '2018-01-01',  -- Начальная дата
        DATE '2099-12-31',  -- Конечная дата
        INTERVAL '1 day'    -- Шаг (1 день)
    ) as calendar_date
 ),
 calendar_row_number as (
	select 
	row_number() over (order by calendar_date) as date_id, 
 	calendar_date as date
 	from gen_calendar
)
select 
row_number() over (order by enable_dttm) as retantion_id,
customer_dim_id, 
cr1.date_id as enable_dttm_id,
cr2.date_id as disable_dttm_id,
service_request_enable_id,
service_request_disable_id,
extract(day from (case when disable_dttm is null then '2021-04-01' else disable_dttm end) - enable_dttm) as number_days,
--- extract(month from age((case when disable_dttm is null then '2021-04-01' else disable_dttm end), enable_dttm)) as number_months
(EXTRACT(YEAR FROM (CASE WHEN disable_dttm IS NULL THEN '2021-04-01'::date ELSE disable_dttm END)) - EXTRACT(YEAR FROM enable_dttm)) * 12 +
(EXTRACT(MONTH FROM (CASE WHEN disable_dttm IS NULL THEN '2021-04-01'::date ELSE disable_dttm END)) - EXTRACT(MONTH FROM enable_dttm)) AS number_months
from activation_preiods_filter
join customer_dim cd on b_customer_id = customer_id
join calendar_row_number cr1 on cr1.date = enable_dttm
join calendar_row_number cr2 on cr2.date = (case when disable_dttm is null then '2099-12-30' else disable_dttm end)
where flag_activation = 1
order by customer_dim_id, enable_dttm
"""


LAST_DATA_UPDATE_DATE_QUERY = f"""
DO $$
BEGIN
    -- Проверяем, существует ли таблица с правильной структурой
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = '{DIM_MODEL_SCHEMA_NAME}'
          AND table_name = '{LAST_DATA_UPDATE_DATE}'
          AND column_name IN ('id', 'success', 'date')

    ) THEN
        -- Если структура верна, ничего не делаем
        RETURN;
    ELSE
        -- Иначе удаляем таблицу и создаем заново
        DROP TABLE IF EXISTS {DIM_MODEL_SCHEMA_NAME}.{LAST_DATA_UPDATE_DATE};
        
        CREATE TABLE {DIM_MODEL_SCHEMA_NAME}.{LAST_DATA_UPDATE_DATE} (
            id SERIAL PRIMARY KEY,
            success BOOL,
            date DATE DEFAULT CURRENT_DATE
        );
    END IF;
END $$;
"""


        #   AND (column_name = 'id' AND data_type = 'integer' AND character_maximum_length IS NULL)
        #   AND (column_name = 'success' AND data_type = 'boolean')
        #   AND (column_name = 'date' AND data_type = 'date')