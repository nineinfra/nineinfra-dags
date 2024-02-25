import airflow
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.dates import days_ago

default_args = {'owner': 'airflow',
                'start_date': airflow.utils.dates.days_ago(2),
                'end_date': datetime.now(),
                'depends_on_past': False,
                'email': ['airflow@example.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 5,
                'retry_delay': timedelta(minutes=5)}

dag_exec_scripts = DAG(dag_id='dag_exec_scripts_demo',
                       default_args=default_args,
                       schedule_interval='@once',
                       start_date=days_ago(1),
                       dagrun_timeout=timedelta(minutes=60),
                       description='executing the sql and hql scripts', )

create_table = SQLExecuteQueryOperator(sql="sql/create_mysql_table.sql", task_id="create_table_task",
                                       conn_id="mysql_demo",
                                       dag=dag_exec_scripts)
load_data = SQLExecuteQueryOperator(sql="sql/load_data_to_mysql.sql", task_id="load_data_task", conn_id="mysql_demo",
                                    dag=dag_exec_scripts)
execute_hive_script = HiveOperator(hql="sql/create_hive_table.hql", task_id="execute_hql_task",
                                   hive_cli_conn_id="hive_demo", dag=dag_exec_scripts)

create_table >> load_data >> execute_hive_script

if __name__ == '__main__ ':
    dag_exec_scripts.cli()
