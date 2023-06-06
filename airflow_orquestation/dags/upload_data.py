from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime
import os


AWS_CONN_ID = "AWSConnection"
PATH = './data/'
BUCKET_RAW = 'eda-dengineer-pipeline-raw'
BUCKET_PRESTAGE = 'eda-dengineer-pipeline-stage'
BUCKET_ANALYTICS = 'eda-dengineer-pipeline-analytics'
JOB_OBS = 'eda-dengineer-observability-glue'
JOB_PRESTAGE = 'eda-dengineer-pre_stage-glue'
JOB_POSTSTAGE = 'eda-dengineer-post_stage-glue'


REGION = 'us-east-1'
FILES = ['hurtos_med_mod.csv']
FILES_ANALYTICS = ['observability/hurtos_med_mod.csv', 
                'observability/cantidad_general.csv', 
                'observability/general_features.csv']


def uploadData(path, bucket):
    conn = S3Hook(aws_conn_id=AWS_CONN_ID)
    client = conn.get_conn()
    files = os.listdir(path)
    for file in files:
        if file.rsplit('.',1)[1]=='csv':
            client.upload_file(
                Filename=path+file,
                Bucket=bucket,
                Key=f'{file}'
                )

glue_arguments = {"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"}

glue_prestage_args = {"--PREFIX": "hurtos_med_mod",
                        "--BUCKET_PRESTAGE":"eda-dengineer-pipeline-stage",
                        "--BUCKET_RAW":"eda-dengineer-pipeline-raw",
                        "--additional-python-modules":"awswrangler==2.17.0"
                        }
glue_observability_args = {"--PREFIX": "hurtos_med_mod",
                        "--BUCKET_ANALYTICS": "eda-dengineer-pipeline-analytics",
                        "--BUCKET_RAW":"eda-dengineer-pipeline-raw",
                        "--additional-python-modules":"great_expectations, awswrangler==2.17.0",
                        "--extra-py-files":"s3://eda-dengineer-pipeline-analytics/dependencies/squema.py,s3://eda-dengineer-pipeline-analytics/dependencies/utils.py"}

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 1),
}



with DAG(dag_id="eda_denigneer_aws",
         description="Eda data engineer using aws and airflow",
         schedule_interval="@once",
         tags=['EDA_DE'],
         default_args=default_args) as dag:
    
    upload_file = PythonOperator(task_id="upload_s3_raw",
                        python_callable=uploadData,
                        op_kwargs={
                            'path': PATH ,
                            'bucket': BUCKET_RAW,
                            }
                        )

    raw_sensor = S3KeySensor(
        task_id='s3_check_raw',
        poke_interval=5,
        timeout=90,
        soft_fail=False,
        retries=2,
        bucket_key=FILES,
        trigger_rule =TriggerRule.ALL_SUCCESS,
        bucket_name=BUCKET_RAW,
        aws_conn_id=AWS_CONN_ID
        )

    job_observability = GlueJobOperator(
        task_id="run_job_observability",
        job_name=JOB_OBS,
        script_location='s3://aws-glue-assets-834399531927-us-east-1/scripts/eda-dengineer-observability-glue.py',
        s3_bucket='aws-glue-assets-834399531927-us-east-1',
        iam_role_name='prueba-GlueRol',
        create_job_kwargs=glue_arguments,
        script_args=glue_observability_args,
        trigger_rule =TriggerRule.ALL_SUCCESS,
        aws_conn_id=AWS_CONN_ID
    )
 
    job_pre_stage = GlueJobOperator(
        task_id="run_job_pre_stage",
        job_name=JOB_PRESTAGE,
        script_location='s3://aws-glue-assets-834399531927-us-east-1/scripts/eda-dengineer-pre_stage-glue.py',
        s3_bucket='aws-glue-assets-834399531927-us-east-1',
        iam_role_name='prueba-GlueRol',
        trigger_rule =TriggerRule.ALL_SUCCESS,
        create_job_kwargs=glue_arguments,
        script_args=glue_prestage_args,
        aws_conn_id=AWS_CONN_ID
    )
    

    prestage_sensor = S3KeySensor(
        task_id='s3_check_prestage',
        poke_interval=5,
        timeout=90,
        soft_fail=False,
        retries=2,
        bucket_key=f'pre-stage/{FILES[0]}',
        trigger_rule =TriggerRule.ALL_SUCCESS,
        bucket_name=BUCKET_PRESTAGE,
        aws_conn_id=AWS_CONN_ID
        )
    

    job_post_stage = GlueJobOperator(
        task_id="run_job_post_stage",
        job_name=JOB_POSTSTAGE,
        script_location='s3://aws-glue-assets-834399531927-us-east-1/scripts/eda-dengineer-post_stage-glue.py',
        s3_bucket='aws-glue-assets-834399531927-us-east-1',
        iam_role_name='prueba-GlueRol',
        create_job_kwargs=glue_arguments,
        script_args=glue_prestage_args,
        trigger_rule =TriggerRule.ALL_SUCCESS,
        aws_conn_id=AWS_CONN_ID
    )

    analytics_sensor = S3KeySensor(
        task_id='s3_check_analytics',
        poke_interval=5,
        timeout=90,
        soft_fail=False,
        retries=2,
        bucket_key=FILES_ANALYTICS,
        trigger_rule =TriggerRule.ALL_SUCCESS,
        bucket_name=BUCKET_ANALYTICS,
        aws_conn_id=AWS_CONN_ID
        )

    upload_file >> raw_sensor >> [job_observability, job_pre_stage] >> prestage_sensor >> job_post_stage >> analytics_sensor

