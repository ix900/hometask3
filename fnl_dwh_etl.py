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
    'start_date': datetime(2012, 1, 1, 0, 0, 0)
}



dag = DAG(
    USERNAME + '_fnl_dwh_etl',
    default_args=default_args,
    description='DLYBIN FINAL DWH ETL tasks ',
    schedule_interval="0 0 1 1 *",
    params={'schemaName': USERNAME},
)

def fill_tables(schemaName="", execute_date="", table_type=""):
    request = """ select tbl_name,tbl_fill_query, tbl_del_query
                  from {0}.f_meta_tables, {0}.f_meta_type
                  where tbl_type_id = type_id and type_name='{1}' order by tbl_id """.format(schemaName, table_type)
    pg_hook = PostgresHook()
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    for tbl_name, tbl_fill_query, tbl_del_query in sources:
        try:
            if tbl_del_query is not None and tbl_fill_query is not None:
                if execute_date is not None:
                    cursor.execute(tbl_del_query.format(schemaName, execute_date))
                else:
                    cursor.execute(tbl_del_query.format(schemaName))
        except Exception as e:
               raise Exception('Ошибка:%s Запрос:%s' % (e, tbl_del_query))
        try:
            if tbl_fill_query is not None:
                if execute_date is not None:
                    cursor.execute(tbl_fill_query.format(schemaName, execute_date))
                else:
                    cursor.execute(tbl_fill_query.format(schemaName))
            else:
                raise Exception("Query for fill %s is empty!" % tbl_name)
        except Exception as e:
               raise Exception('Ошибка:%s Запрос:%s' % (e, tbl_fill_query))
        cursor.execute('commit')


fill_ods_task = PythonOperator(
    task_id="fill_ods_tables",
    python_callable=fill_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_date': '{{ execution_date }}', 'table_type': 'ods'}
)

fill_dds_hub_task = PythonOperator(
    task_id="fill_hub",
    python_callable=fill_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_date': None, 'table_type': 'hub'}
)

fill_dds_link_task = PythonOperator(
    task_id="fill_link",
    python_callable=fill_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_date': None, 'table_type': 'link'}
)

fill_dds_sat_task = PythonOperator(
    task_id="fill_sat",
    python_callable=fill_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_date': '{{ execution_date }}', 'table_type': 'satellite'}
)

fill_dm_tmp_task = PythonOperator(
    task_id="fill_dm_tmp",
    python_callable=fill_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_date': '{{ execution_date }}', 'table_type': 'dmtmp'}
)

fill_dm_dim_task = PythonOperator(
    task_id="fill_dm_dim",
    python_callable=fill_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_date': None, 'table_type': 'dmdim'}
)

fill_dm_fct_task = PythonOperator(
    task_id="fill_dm_fct",
    python_callable=fill_tables,
    dag=dag,
    op_kwargs={'schemaName': '{{ params.schemaName }}', 'execute_date': '{{ execution_date }}', 'table_type': 'dmfct'}
)

start_task = DummyOperator(task_id='start_task', dag=dag)
dds_loaded = DummyOperator(task_id='dds_loaded', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)
ods_loaded = DummyOperator(task_id="ods_loaded", dag=dag)


start_task >> fill_ods_task >> ods_loaded

ods_loaded >> fill_dds_hub_task >> dds_loaded
ods_loaded >> fill_dds_link_task >> dds_loaded
ods_loaded >> fill_dds_sat_task >> dds_loaded

dds_loaded >> fill_dm_tmp_task >> fill_dm_dim_task >> fill_dm_fct_task >> end_task






