from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.operators import bash_operator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators import dummy_operator
import datetime
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator

PROJECT='big-data-environment'

default_dag_args = {
    'start_date': datetime.datetime(2020, 9, 15),
    'provide_context': True
}

with models.DAG(
        dag_id='batch-kpi-scheduled',
        schedule_interval="*/30 * * * *",
        default_args=default_dag_args
) as dag:
    create_dataproc_cluster = DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name="vf-polimi-demo",
        project_id=PROJECT,
        num_workers=2,
        service_account="dataproc-service-account@" + PROJECT + ".iam.gserviceaccount.com",
        master_machine_type="n1-highmem-4",
        worker_machine_type="n1-highmem-4",
        worker_disk_size=50,
        master_disk_size=50,
        image_version="1.4-debian9",
        tags=['default-allow-internal', 'default-allow-ssh'],
        region="europe-west1",
        subnetwork_uri="projects/" + PROJECT + "/regions/europe-west1/subnetworks/default",
        properties={'core:fs.gs.implicit.dir.repair.enable': 'false', 'core:fs.gs.status.parallel.enable' : 'true', 'core:mapreduce.fileoutputcommitter.marksuccessfuljobs' : 'false', 'core:spark.pyspark.python' : 'python3', 'core:spark.pyspark.driver.python' : 'python3'},
        metadata = {'enable-oslogin' : 'true', 'PIP_PACKAGES' : 'google-cloud-pubsub'},
        optional_components=['ANACONDA','JUPYTER','ZEPPELIN'],
        enable_optional_components=True,
        enable_http_port_access=True,
        zone="europe-west1-b",
        storage_bucket="vf-polimi-batch-data",
        idle_delete_ttl=3601,
        internal_ip_only=False,
        init_actions_uris=['gs://goog-dataproc-initialization-actions-europe-west1/python/pip-install.sh']
    )

    run_batch_kpi_scheduled = dataproc_operator.DataProcPySparkOperator(
        task_id="submit_batch-kpi-scheduled",
        cluster_name='vf-polimi-demo',
        region='europe-west1',
        main='gs://vf-polimi-batch-data/dev/compute-kpi-batch.py',
        dataproc_pyspark_jars='gs://spark-lib/bigquery/spark-bigquery-latest.jar',
        xcom_push=True
    )

    remove_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        project_id=PROJECT,
        task_id="delete_cluster",
        cluster_name='vf-polimi-demo',
        region='europe-west1'
    )

    def check_batch_kpi_scheduled_cluster_running(**kwargs):
        ti = kwargs['ti']
        xcom_value = ti.xcom_pull(task_ids='batch_kpi_scheduled_cluster')
        if xcom_value == "vf-polimi-demo":
            return 'delete_cluster'
        else:
            return 'end'

    branch_batch_kpi_scheduled_active_cluster = BranchPythonOperator(
        task_id='check_batch_kpi_scheduled_cluster',
        provide_context=True,
        python_callable=check_batch_kpi_scheduled_cluster_running)

    batch_kpi_scheduled_cluster_running = bash_operator.BashOperator(
        task_id='batch_kpi_scheduled_cluster',
        bash_command="gcloud dataproc clusters list --region europe-west1 | grep 'vf-polimi-demo'| awk '{print $1; exit}'",
        xcom_push=True,
        trigger_rule="all_done")

    end_pipeline = dummy_operator.DummyOperator(
        task_id='end')

    create_dataproc_cluster >> run_batch_kpi_scheduled >> batch_kpi_scheduled_cluster_running >> branch_batch_kpi_scheduled_active_cluster >> [remove_cluster, end_pipeline]