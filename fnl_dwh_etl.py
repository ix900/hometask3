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

def get_ods_tables():
    request = "SELECT table_name FROM information_schema.tables WHERE table_schema='{{ params.schemaName }}' and table_type='BASE TABLE' and table_name like 'f%ods%'"
    pg_hook = PostgresHook()
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    for source in sources:
        print("ods: {0}".format(source[0]))
    return sources

def test():
    print('Hello')

hook_task = PythonOperator(
    task_id="htask",
    python_callable=get_ods_tables,
    dag=dag
)

start_task = DummyOperator(task_id='start_task', dag=dag)
start_task >> hook_task
