from datetime import timedelta, datetime
from random import randint
from itertools import chain
import re

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
all_link_loaded = DummyOperator(task_id="all_link_loaded", dag=dag)
#hubs
hubs_link = {'hub_user': {'fields': ['USER_PK','USER_KEY','LOAD_DATE','RECORD_SOURCE']},
        'hub_billing': {'fields': ['BILLING_PERIOD_PK', 'BILLING_PERIOD_KEY', 'LOAD_DATE', 'RECORD_SOURCE']},
        'hub_paydoctype': {'fields': ['PAY_DOC_TYPE_PK', 'PAY_DOC_TYPE_KEY', 'LOAD_DATE', 'RECORD_SOURCE']},
        'hub_account': {'fields': ['ACCOUNT_PK', 'ACCOUNT_KEY', 'LOAD_DATE', 'RECORD_SOURCE']},
        'link_payment': {'fields': ['PAY_PK','USER_PK', 'ACCOUNT_PK', 'BILLING_PERIOD_PK', 'PAY_DOC_TYPE_PK', 'LOAD_DATE', 'RECORD_SOURCE']}
       }

for h in hubs_link.keys():
    fields = ','.join(hubs_link[h]['fields'])
    fill_hab = PostgresOperator(
        task_id="fill_%s" % h,
        dag=dag,
        sql="""
        insert into {{ params.schemaName }}.dds_%s (%s)
        select %s from {{ params.schemaName }}.dds_%s_etl                          
    """ % (h, fields, fields, h))
    if re.match(r'^hub_.{1,}$', h):
        ods_loaded >> fill_hab >> all_hub_loaded

    if re.match(r'^link_.{1,}$',h):
        ods_loaded >> fill_hab >> all_link_loaded


#sat
all_sat_loaded = DummyOperator(task_id="all_sat_loaded", dag=dag)
sats = {'user_info': {'fields': ['PHONE','EFFECTIVE_DATE','LOAD_DATE','RECORD_SOURCE'],
                      'key': 'USER_PK','hashdiff':'USER_HASHDIFF'
                     }
        ,
        'link_payment': {'fields': ['PAY_PK', 'PAY_HASHDIFF', 'PAY_DOC_NUM', 'PAY_DATE', 'SUM', 'EFFECTIVE_DATE',
                                    'LOAD_DATE', 'RECORD_SOURCE'],
                         'key': 'PAY_PK', 'hashdiff': 'PAY_HASHDIFF'
                         }
        }

listmerge = lambda lst: list(chain(*lst))
for sat in sats.keys():
    fields_sat = listmerge([[sats[sat]['key'], sats[sat]['hashdiff']], sats[sat]['fields']])
    key_sat = sats[sat]['key']
    hash_sat = sats[sat]['hashdiff']
    fill_sat = PostgresOperator(
        task_id="fill_sat_%s" % sat,
        dag=dag,
        sql="""
               INSERT INTO {{ params.schemaName }}.dds_sat_%s (%s) 
               WITH source_data AS (
                    SELECT %s
                    FROM {{ params.schemaName }}.ods_payment_hashed v
                    WHERE v.LOAD_DATE <= '{{ execution_date }}'::TIMESTAMP 
                ),
                     update_records AS (
                         SELECT satt.*
                         FROM {{ params.schemaName }}.dds_sat_%s as satt
                         JOIN source_data as src 
                            ON src.%s = satt.%s AND satt.LOAD_DATE <= (select max(LOAD_DATE) from source_data)
                     ),
                     latest_records AS (
                         SELECT * FROM (
                             SELECT upt.*,
                                    rank() OVER(PARTITION BY upt.%s ORDER BY upt.LOAD_DATE DESC) rank_1
                             FROM update_records as upt
                                       ) as lts
                         WHERE lts.rank_1 = 1
                     )
                         select DISTINCT src.*
                         FROM source_data as src
                         LEFT JOIN latest_records as lts ON lts.%s = src.%s AND lts.%s = src.%s
                         WHERE lts.%s is null
            """ % (sat, fields_sat, fields_sat, sat, key_sat, key_sat, key_sat, key_sat, key_sat, hash_sat, hash_sat, hash_sat  )
    )
    all_hub_loaded >> fill_sat >> all_sat_loaded