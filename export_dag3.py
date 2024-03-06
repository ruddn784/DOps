from __future__ import annotations
from airflow import DAG
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.models import Variable
import os
import io
import datetime
import psycopg2
import boto3
from airflow.utils import dates
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "export_dag2"
ARGS_CONFIG=Variable.get('ARGS_CONFIG', deserialize_json=True)
BUCKET_NAME=Variable.get('bucket')
#AWS_KEY_ID=ARGS_CONFIG['aws_acces_key_id']
#AWS_SECRET_KEY=ARGS_CONFIG['aws_secret_acces_key']
#region_name=''
#ENDPOINT=ARGS_CONFIG['endpoint_url']


session = boto3.session.Session()
rs=session.client(
    #service_name='s3',
    's3', **ARGS_CONFIG
)

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2023, 9, 5),
    schedule="@daily",
    #schedule=timedelta(days=1),
    catchup=False,
) as dag:
    d_begin='2016-03-01'
    #d_end='2016-10-13 17:00:00.000'
    d_end='2016-05-01'
    #yesterday = yesterday_ds_nodash
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(1)
    name = today.strftime("%m-%d-%Y")
    name =name +'.csv'
    #template= open('/opt/airflow/dags/sql/creat_export.txt', 'r')
    query=Variable.get('QUERY_VAR')
    #query=template.read()
    #query=template[template]
    @task(task_id="export_csv")
    def gt_csv (query):
        conn=psycopg2.connect("host=host.docker.internal dbname=demo user=postgres password=example")
        cur=conn.cursor()
        with open(name, 'w') as f:
        #f= io.StringIO()
         cur.copy_expert("COPY (%s) TO STDOUT (FORMAT 'csv')" % query.format(d1=d_begin,d2=d_end),  f, size=1024)
        #DATA = f#.getvalue() 
        conn.commit()
        #return print('date is', yesterday.strftime("%Y-%m-%d %H:%M:%S"),  
        #             today.strftime("%Y-%m-%d %H:%M:%S"))
        #return print(f)
   
    @task(task_id="upload_csv")
   # def upload_csv (bucket_name, object_name):
   #        rs.upload_object(filename=object_name,  bucket_name=bucket_name
    def upload_csv ():
        #print(rs.list_buckets())
        rs.upload_file(name, BUCKET_NAME, name)

    
    run_get = gt_csv(query)
    run_upload = upload_csv()
   # create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date
    run_get >> run_upload