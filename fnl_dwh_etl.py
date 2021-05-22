from datetime import timedelta, datetime
from random import randint
from itertools import chain
import re

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


USERNAME = 'dlybin'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2018, 1, 1, 0, 0, 0)
}



dag = DAG(
    USERNAME + '_fnl_dwh_etl',
    default_args=default_args,
    description='DLYBIN FINAL DWH ETL tasks ',
    schedule_interval="0 0 1 1 *",
    params={'schemaName': USERNAME},
)


def fill_ods_tables(schemaName="", execute_date=""):
    request = "SELECT tbl_name, tbl_fill_query, tbl_del_query FROM {0}.f_meta_ods".format(schemaName)
    pg_hook = PostgresHook()
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    for tbl_name, tbl_fill_query, tbl_del_query in sources:
        try:
            cursor.execute(tbl_del_query.format(schemaName, execute_date))
        except Exception as e:
            raise Exception('Ошибка:%s Запрос:%s' % (e, tbl_del_query))

        try:
            cursor.execute(tbl_fill_query.format(schemaName, execute_date))
        except Exception as e:
            raise Exception('Ошибка:%s Запрос:%s' % (e, tbl_fill_query))

        cursor.execute('commit')

def fill_dds_tables(schemaName="", execute_date="", table_type=""):
    request = """ select distinct tbl_name,tbl_fill_query
                  from {0}.f_meta_tables, {0}.f_meta_type
                  where tbl_type_id = type_id and type_name='{1}' """.format(schemaName, table_type)
    pg_hook = PostgresHook()
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    for tbl_name, tbl_fill_query in sources:
        if len(tbl_fill_query) > 0:
            if execute_date is not None:
                cursor.execute(tbl_fill_query.format(schemaName, execute_date))
            else:
                cursor.execute(tbl_fill_query.format(schemaName))
        else:
            raise Exception("Query for fill %s is empty!" % tbl_name)
        cursor.execute('commit')


fill_ods_task = PythonOperator(
    task_id="fill_ods_tables",
    python_callable=fill_ods_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_date': '{{ execution_date }}'}
)

fill_dds_hub_task = PythonOperator(
    task_id="fill_hub",
    python_callable=fill_dds_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_date': None, 'table_type': 'hub'}
)

fill_dds_link_task = PythonOperator(
    task_id="fill_link",
    python_callable=fill_dds_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_date': None, 'table_type': 'link'}
)

fill_dds_sat_task = PythonOperator(
    task_id="fill_sat",
    python_callable=fill_dds_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_date': '{{ execution_date }}', 'table_type': 'satellite'}
)

start_task = DummyOperator(task_id='start_task', dag=dag)
ods_loaded = DummyOperator(task_id="ods_loaded", dag=dag)
all_loaded = DummyOperator(task_id="all_loaded", dag=dag)


start_task >> fill_ods_task >> ods_loaded
ods_loaded >> fill_dds_hub_task >> all_loaded
ods_loaded >> fill_dds_link_task >> all_loaded
ods_loaded >> fill_dds_sat_task >> all_loaded






