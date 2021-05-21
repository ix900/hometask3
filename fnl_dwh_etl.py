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
        print(tbl_name)
        print(tbl_del_query.format(schemaName, execute_year))
        cursor.execute(tbl_del_query.format(schemaName, execute_year))
        print(tbl_fill_query.format(schemaName, execute_year))
        cursor.execute(tbl_fill_query.format(schemaName, execute_year))

fill_ods_task = PythonOperator(
    task_id="fill_ods_tables",
    python_callable=fill_ods_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_year': '{{ execution_date.year }}'}
)

start_task = DummyOperator(task_id='start_task', dag=dag)
start_task >> fill_ods_task
