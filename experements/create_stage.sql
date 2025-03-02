create table test_tables.CRM_TRANSACTION_TYPE
(
  transaction_type_cd int primary key,
  transaction_type_nm text
);

create table test_tables.CRM_ACCOUNT_TYPE
(
  account_type_cd int primary key,
  account_type_nm text
);

create table test_tables.CRM_ACCOUNT_STATUS
(
  account_status_cd int primary key,
  account_status_nm text
);

create table test_tables.SERVICE_REQUEST_TYPE
(
  service_request_type_cd int primary key,
  service_request_type_nm text
);

create table test_tables.SERVICE_REQUEST_STATUS
(
  service_request_status_cd int primary key,
  service_request_status_nm text
);

create table test_tables.CRM_TRANSACTION
(
  transaction_id int primary key,
  orig_id int,  
  account_id int,
  transaction_type_cd int,
  transaction_amt decimal(12,6),
  transaction_dttm timestamp,
  create_dttm timestamp,
  delete_dttm timestamp
);

create table test_tables.CRM_ACCOUNT
(
  account_id int primary key,
  account_type_cd int,
  acccount_create_dt date,
  account_status_cd int,
  application_id int,
  create_dttm timestamp,
  delete_dttm timestamp
);

create table test_tables.PRODUCT_TYPE
(
  product_type_cd int primary key,
  product_type_nm text
);

create table test_tables.APPLICATION
(
  application_id int primary key,
  product_type_cd int,
  customer_id int,
  advert_source_id int,
  create_dttm timestamp,
  delete_dttm timestamp
);

create table test_tables.CRM_CUSTOMER
(
    crm_customer_id int primary key,
    customer_id int,
    birth_dt date,
    phone_num text,
    email text,
    first_nm text,
    last_nm text,
    create_dttm timestamp,
    delete_dttm timestamp
);

create table test_tables.SERVICE_REQUEST
(
  service_request_id int primary key,
  customer_id int,
  service_request_type_cd int,
  service_request_status_cd int,
  tail_limit int,
  create_dttm timestamp,
  delete_dttm timestamp
);

create table test_tables.ADVERT_SOURCE
(
  advert_source_id int primary key,
  advert_source_nm text,
  monthly_payment_amt decimal(12,6),
  start_month int,
  end_month int,
  create_dttm timestamp,
  delete_dttm timestamp
);

create table test_tables.CAB_CUSTOMER
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
);


alter table test_tables.SERVICE_REQUEST
add foreign key (service_request_type_cd)
references test_tables.SERVICE_REQUEST_TYPE(service_request_type_cd)

alter table test_tables.SERVICE_REQUEST
add foreign key (service_request_status_cd)
references test_tables.SERVICE_REQUEST_STATUS(service_request_status_cd)

alter table test_tables.SERVICE_REQUEST
add foreign key (customer_id)
references test_tables.CAB_CUSTOMER(customer_id)


alter table test_tables.CRM_CUSTOMER
add foreign key (customer_id)
references test_tables.CAB_CUSTOMER(customer_id)

alter table test_tables.APPLICATION
add foreign key (customer_id)
references test_tables.CAB_CUSTOMER(customer_id)


alter table test_tables.APPLICATION
add foreign key (advert_source_id)
references test_tables.ADVERT_SOURCE(advert_source_id)


alter table test_tables.APPLICATION
add foreign key (product_type_cd)
references test_tables.PRODUCT_TYPE(product_type_cd)


alter table test_tables.CRM_ACCOUNT
add foreign key (application_id)
references test_tables.APPLICATION(application_id)


alter table test_tables.CRM_ACCOUNT
add foreign key (account_type_cd)
references test_tables.CRM_ACCOUNT_TYPE(account_type_cd)

alter table test_tables.CRM_ACCOUNT
add foreign key (account_status_cd)
references test_tables.CRM_ACCOUNT_STATUS(account_status_cd)

alter table test_tables.CRM_TRANSACTION
add foreign key (account_id)
references test_tables.CRM_ACCOUNT(account_id)


alter table test_tables.CRM_TRANSACTION
add foreign key (transaction_type_cd)
references test_tables.CRM_TRANSACTION_TYPE(transaction_type_cd)




















