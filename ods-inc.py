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

sys.path.append(os.path.join(os.path.dirname(__file__), "scripts"))
from gensql import generate_etl2ods_inc_sql

job_type = "ods"
ods_type = "inc"  # full, inc
db_type = "mysql"
dag_id_name = f'nineinfra-{job_type}-{ods_type}'

ods_inc_args = {'owner': 'airflow',
                'start_date': airflow.utils.dates.days_ago(1),
                'end_date': datetime.now(),
                'depends_on_past': False,
                'email': ['ninebigbig@gmail.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 5,
                'retry_delay': timedelta(minutes=5)}

dag_ods_inc = DAG(dag_id=f'{dag_id_name}',
                  default_args=ods_inc_args,
                  schedule_interval='@once',
                  start_date=days_ago(1),
                  dagrun_timeout=timedelta(minutes=60),
                  description='executing the sql and hql scripts for the ods full', )

generate_sql = PythonOperator(task_id="generate_sql_task",
                              python_callable=generate_etl2ods_inc_sql,
                              op_kwargs={'datahouse_dir': Variable.get("datahouse_dir"),
                                         'start_date': airflow.utils.dates.days_ago(1).date()},
                              provide_context=True,
                              dag=dag_ods_inc)
load_data = HiveOperator(hql=f'sqls/etl2ods_{ods_type}.sql',
                         task_id="load_data_task",
                         hive_cli_conn_id="hive_conn",
                         dag=dag_ods_inc)

generate_sql >> load_data
