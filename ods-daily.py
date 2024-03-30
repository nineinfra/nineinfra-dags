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
import gensql

job_type = "ods"
sync_type = "daily"  # full, inc, daily
dag_id_name = f'nineinfra-{job_type}-{sync_type}'

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
                   start_date=days_ago(0),
                   dagrun_timeout=timedelta(minutes=60),
                   description=f'executing the sql and hql scripts for the {job_type} {sync_type}', )

generate_sql = PythonOperator(task_id="generate_sql_task",
                              python_callable=gensql.generate_etl2ods_daily_sql,
                              op_kwargs={'datahouse_dir': Variable.get("datahouse_dir"),
                                         'start_date': airflow.utils.dates.days_ago(0).date()},
                              provide_context=True,
                              trigger_rule='all_done',
                              dag=dag_instance)
load_data = HiveOperator(hql=f'sqls/etl2ods_{sync_type}.sql',
                         task_id="load_data_task",
                         hive_cli_conn_id="hive_conn",
                         trigger_rule='all_done',
                         dag=dag_instance)

generate_sql >> load_data
