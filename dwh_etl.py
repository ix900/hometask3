from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'dlybin'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2019, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
)

all_hub_loaded = DummyOperator(task_id="all_hub_loaded", dag=dag)

all_link_loaded = DummyOperator(task_id="all_link_loaded", dag=dag)

all_sat_loaded = DummyOperator(task_id="all_sat_loaded", dag=dag)

