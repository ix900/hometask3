from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'dlybin'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2019, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl',
    default_args=default_args,
    description='DLYBIN DWH ETL tasks ',
    schedule_interval="0 0 1 1 *",
    params={'schemaName': USERNAME},
)

clear_ods = PostgresOperator(
    task_id="clear_ods",
    dag=dag,
    sql="""
        DELETE FROM {{ params.schemaName }}.ods_payment WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)

fill_ods = PostgresOperator(
    task_id="fill_ods",
    dag=dag,
    sql="""
        INSERT INTO {{ params.schemaName }}.ods_payment SELECT * FROM {{ params.schemaName }}.stg_payment 
        WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)

create_v_payment = PostgresOperator(
    task_id="create_view",
    dag=dag,
    sql="""
        create or replace view dlybin.ods_v_payment as (
            with staging_payment as (
                with derived_columns as (
                    select user_id,
                       pay_doc_type,
                       pay_doc_num,
                       account,
                       phone,
                       billing_period,
                       pay_date,
                       sum,
                       user_id::TEXT as USER_KEY,
                       account::TEXT as ACCOUNT_KEY,
                       billing_period::TEXT as BILLING_PERIOD_KEY,
                       pay_doc_type::TEXT as PAY_DOC_TYPE_KEY,
                       'PAYMENT-DATA-LAKE'::TEXT as RECORD_SOURCE
                    FROM dlybin.ods_payment        
                ),
                hashed_columns AS (
                    select
                    user_id,
                    pay_doc_type,
                    pay_doc_num,
                    account,
                    phone,
                    billing_period,
                    pay_date,
                    sum,
                    USER_KEY,
                    ACCOUNT_KEY,
                    BILLING_PERIOD_KEY,
                    PAY_DOC_TYPE_KEY,
                    RECORD_SOURCE,
                    CAST((MD5(NULLIF(UPPER(TRIM(CAST(user_id as varchar))),''))) as TEXT) AS USER_PK,
                    CAST((MD5(NULLIF(UPPER(TRIM(CAST(account as varchar))),''))) as TEXT) AS ACCOUNT_PK,
                    CAST((MD5(NULLIF(UPPER(TRIM(CAST(billing_period as varchar))),''))) as TEXT) AS BILLING_PERIOD_PK,
                    CAST((MD5(NULLIF(UPPER(TRIM(CAST(pay_doc_type as varchar))),''))) as TEXT) AS PAY_DOC_TYPE_PK,
            
                    CAST(MD5(NULLIF(CONCAT_WS('||',
                         COALESCE(NULLIF(UPPER(TRIM(USER_KEY)),''),'^^'),
                         COALESCE(NULLIF(UPPER(TRIM(ACCOUNT_KEY)),''),'^^'),
                         COALESCE(NULLIF(UPPER(TRIM(BILLING_PERIOD_KEY)),''),'^^'),
                         COALESCE(NULLIF(UPPER(TRIM(PAY_DOC_TYPE_KEY)),''),'^^')
                    ),'^^||^^||^^')) AS TEXT) AS PAY_PK,
                    CAST(MD5(CONCAT_WS('||',
                        COALESCE(NULLIF(UPPER(TRIM(CAST(phone as VARCHAR))),''),'^^'))) AS TEXT) AS USER_HASHDIFF,
                    CAST(MD5(CONCAT_WS('||',
                        COALESCE(NULLIF(UPPER(TRIM(CAST(PAY_DOC_NUM as VARCHAR))),''),'^^'),
                        COALESCE(NULLIF(UPPER(TRIM(CAST(PAY_DATE as VARCHAR))),''),'^^'),
                        COALESCE(NULLIF(UPPER(TRIM(CAST(SUM as VARCHAR))),''),'^^')
                        )) AS TEXT) AS PAY_HASHDIFF
                    FROM derived_columns
                ),
                 columns_to_select AS (
                     select
                     user_id,
                     pay_doc_type,
                     pay_doc_num,
                     account,
                     phone,
                     billing_period,
                     pay_date,
                     sum,
                     USER_KEY,
                     ACCOUNT_KEY,
                     BILLING_PERIOD_KEY,
                     PAY_DOC_TYPE_KEY,
                     RECORD_SOURCE,
                     USER_PK,
                     ACCOUNT_PK,
                     BILLING_PERIOD_PK,
                     PAY_DOC_TYPE_PK,
                     PAY_PK,
                     PAY_HASHDIFF,
                     USER_HASHDIFF
                     from hashed_columns
                 )

            SELECT * FROM columns_to_select
    )
    SELECT *, '{{ execution_date }}'::TIMESTAMP as LOAD_DATE,
           pay_date as EFFECTIVE_FROM
    FROM staging_payment
)
    """
)

ods_loaded = DummyOperator(task_id="ods_loaded", dag=dag)

clear_ods >> fill_ods >> create_v_payment >> ods_loaded

#hubs
hubs = {'user': {'fields': ['USER_PK','USER_KEY','LOAD_DATE','RECORD_SOURCE']},
        'billing': {'fields': ['BILLING_PERIOD_PK', 'BILLING_PERIOD_KEY', 'LOAD_DATE', 'RECORD_SOURCE']},
        'paydoctype': {'fields': ['PAY_DOC_TYPE_PK', 'PAY_DOC_TYPE_KEY', 'LOAD_DATE', 'RECORD_SOURCE']},
        'account': {'fields': ['ACCOUNT_PK', 'ACCOUNT_KEY', 'LOAD_DATE', 'RECORD_SOURCE']}
       }

for h in hubs.keys():
    fields = ','.join(hubs[h]['fields'])
    fill_hab = PostgresOperator(
        task_id="fill_hub_%s" % h,
        dag=dag,
        sql="""
        insert into {{ params.schemaName }}.dds_hub_%s (%s)
        select %s from {{ params.schemaName }}.dds_hub_%s_etl                          
    """ % (h, fields, fields, h))

all_hub_loaded = DummyOperator(task_id="all_hub_loaded", dag=dag)

ods_loaded >> fill_hab >> all_hub_loaded


all_link_loaded = DummyOperator(task_id="all_link_loaded", dag=dag)

all_sat_loaded = DummyOperator(task_id="all_sat_loaded", dag=dag)

