from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'dlybin'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2012, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_data_lake_etl',
    default_args=default_args,
    description='Data Lake ETL tasks',
    schedule_interval="0 0 1 1 *",
)

ods_billing = DataProcHiveOperator(
    task_id='ods_billing',
    dag=dag,
    query="""
        insert overwrite table dlybin.ods_billing partition (year={{ execution_date.year }}) 
        select user_id,cast(replace(billing_period,"-","") as int),service,tariff,sum,created_at from dlybin.stg_billing where year(created_at) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_billing_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_issue = DataProcHiveOperator(
    task_id='ods_issue',
    dag=dag,
    query="""
        insert overwrite table dlybin.ods_issue partition (year={{ execution_date.year }}) 
        select user_id,cast(start_time as timestamp),cast(end_time as timestamp),title,description,service from dlybin.stg_issue where year(end_time) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_issue_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)