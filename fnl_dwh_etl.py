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


def fill_ods_tables(schemaName="", execute_year=""):
    request = "SELECT tbl_name, tbl_fill_query, tbl_del_query FROM {0}.f_meta_ods".format(schemaName)
    pg_hook = PostgresHook()
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    for tbl_name, tbl_fill_query, tbl_del_query in sources:
        cursor.execute(tbl_del_query.format(schemaName, execute_year))
        cursor.execute(tbl_fill_query.format(schemaName, execute_year))
        cursor.execute('commit')

def fill_dds_tables(schemaName="", execute_year="", table_type=""):
    request = """ select distinct tbl_name,tbl_fill_query
                  from {0}.f_meta_tables, {0}.f_meta_type
                  where tbl_id = fld_tbl_id and tbl_type_id = type_id and type_name='{1}' """.format(schemaName, table_type)
    pg_hook = PostgresHook()
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    for tbl_name, tbl_fill_query in sources:
        if execute_year is not None:
            cursor.execute(tbl_fill_query.format(schemaName, execute_year))
        else:
            cursor.execute(tbl_fill_query.format(schemaName))
        cursor.execute('commit')


fill_ods_task = PythonOperator(
    task_id="clear_and_fill_ods_tables",
    python_callable=fill_ods_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_year': '{{ execution_date.year }}'}
)

fill_dds_hub_task = PythonOperator(
    task_id="fill_hub",
    python_callable=fill_dds_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_year': None, 'table_type': 'hub'}
)

fill_dds_link_task = PythonOperator(
    task_id="fill_link",
    python_callable=fill_dds_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_year': None, 'table_type': 'link'}
)

fill_dds_sat_task = PythonOperator(
    task_id="fill_sat",
    python_callable=fill_dds_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_year': '{{ execution_date.year }}', 'table_type': 'satellite'}
)

start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='start_task', dag=dag)
ods_loaded = DummyOperator(task_id="ods_loaded", dag=dag)
all_hub_loaded = DummyOperator(task_id="all_hub_loaded", dag=dag)
all_sat_loaded = DummyOperator(task_id="all_sat_loaded", dag=dag)
all_link_loaded = DummyOperator(task_id="all_link_loaded", dag=dag)

start_task >> fill_ods_task >> ods_loaded
ods_loaded >> fill_dds_hub_task >> all_hub_loaded
ods_loaded >> fill_dds_link_task >> all_link_loaded
ods_loaded >> fill_dds_sat_task >> all_sat_loaded



