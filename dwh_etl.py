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

clear_payment_hashed = PostgresOperator(
    task_id="clear_payment_hashed",
    dag=dag,
    sql="""        
        DELETE FROM  {{ params.schemaName }}.ods_payment_hashed          
         WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}    

    """
)


fill_payment_hashed = PostgresOperator(
    task_id="fill_payment_hashed",
    dag=dag,
    sql="""        
        INSERT INTO {{ params.schemaName }}.ods_payment_hashed
         SELECT *, '{{ execution_date }}'::TIMESTAMP FROM dlybin.ods_v_payment 
         WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}    

    """
)

ods_loaded = DummyOperator(task_id="ods_loaded", dag=dag)

clear_ods >> fill_ods >> clear_payment_hashed >> fill_payment_hashed >> ods_loaded

all_hub_loaded = DummyOperator(task_id="all_hub_loaded", dag=dag)

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
    ods_loaded >> fill_hab >> all_hub_loaded






all_link_loaded = DummyOperator(task_id="all_link_loaded", dag=dag)

all_sat_loaded = DummyOperator(task_id="all_sat_loaded", dag=dag)

