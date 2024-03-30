import os
import sys
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.append(os.path.join(os.path.dirname(__file__)))
import seatunneljob

job_type = "etl"
etl_type = "full"  # full, inc
source = "mysql"
sink = "minio"
dag_prefix = f'nineinfra-{job_type}-{etl_type}-'
seatunneljobs_subdir = f'seatunneljobs/{job_type}/{etl_type}/{source}2{sink}'


def generate_seatunneljob_name(index):
    prefix = f'nineinfra-seatunneljob-{job_type}-{etl_type}-{source}2{sink}-'
    suffix = str(index)
    return prefix + suffix


def create_dynamic_dag(name_space, name, index, yaml_file_name):
    dag_args = {'owner': 'airflow',
                'start_date': datetime.now(),
                'end_date': datetime.now() + timedelta(days=1),
                'depends_on_past': False,
                'email': ['ninebigbig@gmail.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 5,
                'retry_delay': timedelta(minutes=5)}
    table_name = os.path.splitext(os.path.basename(yaml_file_name))[0]
    with DAG(dag_id=f'{dag_prefix}{source}2{sink}-{index}-{table_name}',
             default_args=dag_args,
             description='Dynamically create and monitor seatunneljobs in Kubernetes from YAML files',
             schedule=timedelta(days=1),
             ) as dag:
        def create_seatunneljob_task(ns, n, y):
            return PythonOperator(
                task_id=f'create-{n}',
                python_callable=seatunneljob.create_seatunneljob,
                op_kwargs={'namespace': ns, 'name': n, 'yaml_file': y},
                provide_context=True,
                trigger_rule='all_done',
                dag=dag,
            )

        def monitor_seatunneljob_task(ns, n):
            return PythonOperator(
                task_id=f'monitor-{n}',
                python_callable=seatunneljob.monitor_seatunneljob,
                op_kwargs={'namespace': ns, 'name': n},
                provide_context=True,
                trigger_rule='all_done',
                dag=dag,
            )

        def delete_seatunneljob_task(ns, n):
            return PythonOperator(
                task_id=f'delete-{n}',
                python_callable=seatunneljob.delete_seatunneljob,
                op_kwargs={'namespace': ns, 'name': n},
                provide_context=True,
                trigger_rule='all_done',
                dag=dag,
            )

        create_task = create_seatunneljob_task(name_space, name, yaml_file_name)
        monitor_task = monitor_seatunneljob_task(name_space, name)
        delete_task = delete_seatunneljob_task(name_space, name)

        create_task >> monitor_task >> delete_task

    return dag


task_index = 0
namespace = Variable.get("namespace")
current_dir = os.path.dirname(os.path.realpath(__file__))
yaml_files_dir = os.path.join(current_dir, f'{seatunneljobs_subdir}')
yaml_files = [f for f in os.listdir(yaml_files_dir) if f.endswith('.yaml')]
for yaml_file in yaml_files:
    task_index += 1
    job_name = generate_seatunneljob_name(task_index)
    yaml_file_path = os.path.join(yaml_files_dir, yaml_file)
    globals()[job_name] = create_dynamic_dag(name_space=namespace, name=job_name, index=task_index,yaml_file_name=yaml_file_path)
