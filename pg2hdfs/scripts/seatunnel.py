import string
import random
import time

from kubernetes import client, config
from kubernetes.client.rest import ApiException

config.load_incluster_config()

# 创建Kubernetes API客户端
corev1 = client.CoreV1Api()
rbacv1 = client.RbacAuthorizationV1Api()

# 全局变量
namespace = "dwh"
seatunnel_resource_name = "nine-seatunnel"
seatunnel_pod_name = seatunnel_resource_name + "-" + ''.join(
    random.choices(string.ascii_letters.lower() + string.digits, k=12))
seatunnel_configmap_name = "nine-test-nine-pg2hdfs"
seatunnel_mount_path = "/opt/seatunnel/conf"
spark_configmap_name = "nine-test-nine-kyuubi-clusterrefs"
spark_mount_path = "/opt/spark/conf"
spark_config_file = "spark-defaults.conf"
hdfssite_config_file = "hdfs-site.xml"
coresite_config_file = "core-site.xml"
hivesite_config_file = "hive-site.xml"
config_file_path = "/opt/seatunnel/conf/nine-test-nine-pg2hdfs.conf"
seatunnel_image = "nineinfra/seatunnel:v2.3.4"

# 创建pod的相关资源
seatunnel_role = client.V1Role(
    api_version="rbac.authorization.k8s.io/v1",
    kind="Role",
    metadata=client.V1ObjectMeta(
        name=seatunnel_resource_name,
    ),
    rules=[client.V1PolicyRule(
        api_groups=[""],
        resources=["pods", "configmaps", "services", "persistentvolumeclaims"],
        verbs=["get", "create", "list", "delete", "watch", "deletecollection"]
    )]
)
seatunnel_serviceaccount = client.V1ServiceAccount(
    api_version="v1",
    kind="ServiceAccount",
    metadata=client.V1ObjectMeta(
        name=seatunnel_resource_name,
    )
)
seatunnel_rolebinding = client.V1RoleBinding(
    api_version="rbac.authorization.k8s.io/v1",
    kind="RoleBinding",
    metadata=client.V1ObjectMeta(
        name=seatunnel_resource_name,
    ),
    subjects=[client.RbacV1Subject(
        kind="ServiceAccount",
        name=seatunnel_resource_name,
        namespace=namespace,
    )],
    role_ref=client.V1RoleRef(
        api_group="rbac.authorization.k8s.io",
        kind="Role",
        name=seatunnel_resource_name,
    )
)
seatunnel_pod = client.V1Pod(
    api_version="v1",
    kind="Pod",
    metadata=client.V1ObjectMeta(
        name=seatunnel_pod_name,
    ),
    spec=client.V1PodSpec(
        containers=[
            client.V1Container(
                name=seatunnel_resource_name,
                image=seatunnel_image,
                command=["/opt/seatunnel/bin/start-seatunnel-spark-3-connector-v2.sh", "--master",
                         "k8s://https://10.96.0.1:443", "--config", "/opt/seatunnel/conf/nine-test-nine-pg2hdfs.conf"],
                volume_mounts=[
                    client.V1VolumeMount(
                        name="seatunnel-config",
                        mount_path=seatunnel_mount_path,
                    ),
                    client.V1VolumeMount(
                        name="spark-config",
                        mount_path=spark_mount_path + "/" + spark_config_file,
                        sub_path=spark_config_file,
                    ),
                    client.V1VolumeMount(
                        name="spark-config",
                        mount_path=spark_mount_path + "/" + hdfssite_config_file,
                        sub_path=hdfssite_config_file,
                    ),
                    client.V1VolumeMount(
                        name="spark-config",
                        mount_path=spark_mount_path + "/" + coresite_config_file,
                        sub_path=coresite_config_file,
                    ),
                    client.V1VolumeMount(
                        name="spark-config",
                        mount_path=spark_mount_path + "/" + hivesite_config_file,
                        sub_path=hivesite_config_file,
                    ),
                ],
                env=[
                    client.V1EnvVar(
                        name="POD_IP",
                        value_from=client.V1EnvVarSource(
                            field_ref=client.V1ObjectFieldSelector(
                                field_path="status.podIP",
                            ),
                        ),
                    ),
                    client.V1EnvVar(
                        name="POD_NAME",
                        value_from=client.V1EnvVarSource(
                            field_ref=client.V1ObjectFieldSelector(
                                field_path="metadata.name",
                            ),
                        ),
                    ),
                    client.V1EnvVar(
                        name="NAMESPACE",
                        value_from=client.V1EnvVarSource(
                            field_ref=client.V1ObjectFieldSelector(
                                field_path="metadata.namespace",
                            ),
                        ),
                    ),
                    client.V1EnvVar(
                        name="POD_UID",
                        value_from=client.V1EnvVarSource(
                            field_ref=client.V1ObjectFieldSelector(
                                field_path="metadata.uid",
                            ),
                        ),
                    ),
                    client.V1EnvVar(
                        name="HOST_IP",
                        value_from=client.V1EnvVarSource(
                            field_ref=client.V1ObjectFieldSelector(
                                field_path="status.hostIP",
                            ),
                        ),
                    ),
                ]
            )
        ],
        restart_policy="Never",
        service_account=seatunnel_resource_name,
        volumes=[
            client.V1Volume(
                name="seatunnel-config",
                config_map=client.V1ConfigMapVolumeSource(
                    name=seatunnel_configmap_name,
                )
            ),
            client.V1Volume(
                name="spark-config",
                config_map=client.V1ConfigMapVolumeSource(
                    name=spark_configmap_name,
                    items=[
                        client.V1KeyToPath(
                            key=spark_config_file,
                            path=spark_config_file,
                        ),
                        client.V1KeyToPath(
                            key=hdfssite_config_file,
                            path=hdfssite_config_file,
                        ),
                        client.V1KeyToPath(
                            key=coresite_config_file,
                            path=coresite_config_file,
                        ),
                        client.V1KeyToPath(
                            key=hivesite_config_file,
                            path=hivesite_config_file,
                        ),
                    ]
                )
            )
        ]
    )
)


def monitor_task_status():
    while True:
        try:
            pod = corev1.read_namespaced_pod_status(name=seatunnel_pod_name, namespace=namespace)
            pod_status = pod.status
            if pod_status.phase == "Succeeded":
                return "Success"
            elif pod_status.phase == "Failed":
                return "Failed"
            else:
                time.sleep(5)
        except ApiException as e:
            return f"Error retrieving pod status:"+e


def run_seatunnel_task():
    try:
        corev1.create_namespaced_service_account(namespace=namespace, body=seatunnel_serviceaccount)
    except ApiException as e:
        if e.status != 409:
            raise e

    try:
        rbacv1.create_namespaced_role(namespace=namespace, body=seatunnel_role)
    except ApiException as e:
        if e.status != 409:
            raise e

    try:
        rbacv1.create_namespaced_role_binding(namespace=namespace, body=seatunnel_rolebinding)
    except ApiException as e:
        if e.status != 409:
            raise e

    try:
        corev1.create_namespaced_pod(body=seatunnel_pod, namespace=namespace)
    except ApiException as e:
        if e.status != 409:
            raise e


# 创建pod运行任务
def sync_run_seatunnel_task():
    run_seatunnel_task()
    return monitor_task_status()


# ret = sync_run_seatunnel_task()
# print("run seatunnel task ",ret)
