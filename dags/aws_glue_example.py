from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_hello():
    return 'Hello hello!'

def print_dummy():
    return 'Hello dummy!'

dag = DAG('hello_world', description='Simple glue DAG',
          schedule_interval='@hourly',
          start_date=datetime(2018, 6, 28), catchup=False)

awsGlueOperator = AWSGlueJobOperator(job_name='FIRST_JOB',
script_location='https://s3.us-east-2.amazonaws.com/aws-glue-scripts-001264113720-us-east-2/root/FIRST_JOB',
region_name='us-east-2',
s3_bucket='https://s3.console.aws.amazon.com/s3/', 
iam_role_name='AWSGlueServiceRole-Craw', 
dag=dag)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

awsGlueOperator >> hello_operator

