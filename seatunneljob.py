import string
import random
import time
import yaml
from kubernetes import client, config
from kubernetes.client.rest import ApiException

group = 'seatunnel.nineinfra.tech'
version = 'v1'
plural = 'seatunneljobs'
config.load_incluster_config()
customApi = client.CustomObjectsApi()


def generate_random_string(length):
    letters_and_digits = string.ascii_letters + string.digits
    return ''.join(random.choice(letters_and_digits) for i in range(length))


def generate_seatunneljob_name():
    prefix = 'nineinfra-seatunneljob-'
    suffix = generate_random_string(5)
    return prefix + suffix


def get_seatunneljob_status(ns, n):
    try:
        resource = customApi.get_namespaced_custom_object(group, version, ns, plural, n)
        status = resource.get('status')
        if status is None:
            return False, "NoStatus"
        if status.get('completed') is None:
            return False, "Error"
        else:
            return status.get('completed'), "Success"
    except ApiException as e:
        if e.status == 404:
            return False, "NotFound"
        else:
            print(f"Error getting Custom Resource status: {e}")
            return False, "Error"


def delete_seatunneljob(namespace, name):
    try:
        print(f"Deleting seatunneljob {name} in namespace {namespace}")
        return customApi.delete_namespaced_custom_object(group=group,
                                                         version=version,
                                                         plural=plural,
                                                         namespace=namespace,
                                                         name=name)
    except Exception as e:
        print(f"Error deleting seatunneljob: {e}")


def monitor_seatunneljob(namespace, name):
    while True:
        completed, error = get_seatunneljob_status(namespace, name)
        if error == "NotFound":
            print(f"SeatunnelJob {name} is not found. Retrying...")
            time.sleep(5)
        elif error == "NoStatus":
            print(f"SeatunnelJob {name} has no status yet. Retrying...")
            time.sleep(5)
        elif error == "Error":
            return f"Error getting Custom Resource status"
        else:
            if completed:
                print(f"SeatunnelJob {name} is completed.")
                return "Success"
            else:
                print(f"SeatunnelJob {name} is not completed. Retrying...")
                time.sleep(5)


def create_seatunneljob(namespace, name, yaml_file):
    try:
        print(f"Creating seatunneljob {name} with yaml file {yaml_file} in namespace {namespace}")
        with open(yaml_file, 'r') as file:
            yaml_data = yaml.safe_load(file)
        print(f"Load yaml file data:{yaml_data}")
        yaml_data['metadata']['name'] = name
        return customApi.create_namespaced_custom_object(group=group,
                                                         version=version,
                                                         plural=plural,
                                                         namespace=namespace,
                                                         body=yaml_data)
    except Exception as e:
        print(f"Error creating seatunneljob: {e}")


if __name__ == '__main__':
    create_seatunneljob(namespace='dwh', name='seatunneljob-1',
                        yaml_file='/opt/airflow/dags/nineinfra-dags/seatunneljobs/mysql2minio/table_base_category1.yaml')
    monitor_seatunneljob(namespace='dwh', name='seatunneljob-1')
    delete_seatunneljob(namespace='dwh', name='seatunneljob-1')
