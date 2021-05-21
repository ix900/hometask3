from datetime import timedelta, datetime
from random import randint
from itertools import chain
import re

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


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
    request = "SELECT tbl_name FROM {{ params.schemaName }}.f_meta_tables WHERE tbl_name like 'f%ods%'"
    pg_hook = PostgresHook(schema="dlybin")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    for source in sources:
        print("Source: {0} - ctivate: {1}".format(source[0],source[1]))
    return sources

hook_task = PythonOperator(
    task_id="htask",
    python_callable=get_ods_tables
)

hook_task
