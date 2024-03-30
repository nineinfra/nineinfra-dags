import os
import sys
import airflow
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.apache.hive.operators.hive import HiveOperator

dag_id_name = f'nineinfra-execute-temp-sqls'

dag_args = {'owner': 'airflow',
            'start_date': airflow.utils.dates.days_ago(1),
            'end_date': datetime.now(),
            'depends_on_past': False,
            'email': ['ninebigbig@gmail.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 5,
            'retry_delay': timedelta(minutes=5)}

dag_instance = DAG(dag_id=f'{dag_id_name}',
                   default_args=dag_args,
                   schedule_interval='@once',
                   start_date=days_ago(1),
                   dagrun_timeout=timedelta(minutes=60),
                   description=f'executing the temp sql and hql scripts', )


execute_temp_sql = HiveOperator(hql=f"sqls/temp.sql",
                                task_id=f"execute_temp_sql_task",
                                hive_cli_conn_id="hive_conn",
                                trigger_rule='all_done',
                                dag=dag_instance)

execute_temp_sql
