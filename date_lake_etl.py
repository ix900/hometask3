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

ods_payment = DataProcHiveOperator(
    task_id='ods_payment',
    dag=dag,
    query="""
        insert overwrite table dlybin.ods_payment partition (year={{ execution_date.year }}) 
        select user_id,pay_doc_type, cast(pay_doc_num as int), account, phone, cast(replace(billing_period,"-","") as int), cast(pay_date as timestamp),cast(sum as decimal(20,2))
        from dlybin.stg_payment where year(pay_date) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_payment_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_traffic = DataProcHiveOperator(
    task_id='ods_traffic',
    dag=dag,
    query="""
       insert overwrite table dlybin.ods_traffic partition (year={{ execution_date.year }}) 
       select user_id,cast(from_unixtime(cast(`timestamp`/1000 as bigint)) as timestamp), device_id,device_ip_addr, cast(bytes_sent as int),cast(bytes_received as int) 
       from dlybin.stg_traffic where year(from_unixtime(cast(`timestamp`/1000 as bigint))) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_traffic_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

dm_traffic = DataProcHiveOperator(
    task_id='dm_traffic',
    dag=dag,
    query="""
       insert overwrite table dlybin.dm_traffic partition (year={{ execution_date.year }}) select user_id,max(bytes_received), min(bytes_received), avg(bytes_received), year(event_ts) 
       from dlybin.ods_traffic where year(event_ts) = {{ execution_date.year }} group by user_id,year(event_ts);
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_dm_traffic_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)