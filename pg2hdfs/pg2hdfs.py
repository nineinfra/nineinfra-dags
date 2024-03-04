import os
import sys
import airflow
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

current_dir = os.path.dirname(os.path.realpath(__file__))
sub_dir = os.path.join(current_dir, "scripts")
sys.path.append(sub_dir)
from seatunnel import sync_run_seatunnel_task

pg2hdfs_args = {'owner': 'airflow',
                'start_date': airflow.utils.dates.days_ago(2),
                'end_date': datetime.now(),
                'depends_on_past': False,
                'email': ['ninebigbig@gmail.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 5,
                'retry_delay': timedelta(minutes=5)}

dag_pg2hdfs = DAG(dag_id='nineinfra_pg2hdfs_demo',
                       default_args=pg2hdfs_args,
                       # schedule_interval='0 0 * * *',
                       schedule_interval='@once',
                       start_date=days_ago(1),
                       dagrun_timeout=timedelta(minutes=60),
                       description='executing the sql and hql scripts', )

create_table = SQLExecuteQueryOperator(sql="sql/create_postgresql_table.sql", task_id="create_table_task",
                                       conn_id="postgresql_demo", dag=dag_pg2hdfs)
load_data = SQLExecuteQueryOperator(sql="sql/load_data_to_postgresql.sql", task_id="load_data_task",
                                    conn_id="postgresql_demo", autocommit=True, dag=dag_pg2hdfs)

etl_pg2hdfs = PythonOperator(python_callable=sync_run_seatunnel_task, task_id="etl_pg2hdfs", dag=dag_pg2hdfs)

execute_hive_script = HiveOperator(hql="sql/create_hive_table.hql", task_id="execute_hql_task",
                                   hive_cli_conn_id="hive_demo", dag=dag_pg2hdfs)

create_table >> load_data >> etl_pg2hdfs >> execute_hive_script

if __name__ == '__main__ ':
    dag_pg2hdfs.cli()
