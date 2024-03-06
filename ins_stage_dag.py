from __future__ import annotations
from airflow import DAG
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
import io
import datetime
import psycopg2
import boto3
from psycopg2 import Error
from io import StringIO
from airflow.utils import dates
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "read_bucket"
ARGS_CONFIG=Variable.get('ARGS_CONFIG', deserialize_json=True)
BUCKET_NAME=Variable.get('bucket')
DATA=''
#AWS_KEY_ID=ARGS_CONFIG['aws_acces_key_id']
#AWS_SECRET_KEY=ARGS_CONFIG['aws_secret_acces_key']
#region_name=''
#ENDPOINT=ARGS_CONFIG['endpoint_url']



with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2023, 9, 5),
    schedule="@daily",
    #schedule=timedelta(days=1),
    catchup=False,
) as dag:
  
    session = boto3.session.Session()
    rs=session.client(
     #service_name='s3',
    's3', **ARGS_CONFIG)



    bucket_name=BUCKET_NAME
    forDeletion=[]
    for key in rs.list_objects(Bucket=bucket_name)['Contents']:
        name=key['Key']
        forDeletion.append({'Key':name.strip()})

    get_object_response = rs.get_object(Bucket=bucket_name, Key=name)
    data = get_object_response['Body'].read()
    
       
    @task(task_id="read_csv")
    def pull_csv (data):
       # for key in rs.list_objects(Bucket=bucket_name)['Contents']:
        #    name=key['Key']

        #get_object_response = rs.get_object(Bucket=bucket_name, Key=name)
        #data = get_object_response['Body'].read()
        str_data = data.decode('utf-8')
        #DATA=str_data[284:]
        str_data[284:]
        conn = psycopg2.connect("host=rc1b-500h1nnsu0agprlc.mdb.yandexcloud.net dbname=nds user=user1 password=db1user1 port=6432")
        cur = conn.cursor()
        try:
            print(str_data)
            cur.execute("SELECT insert_csv_data  (%s)",(str_data,))
            conn.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
        finally:
            if conn:
                cur.close()
                conn.close()
                print("Соединение с PostgreSQL закрыто")
    
    @task(task_id="del_csv")
    def del_csv (forDeletion, rs, bucket_name):
        #for key in rs.list_objects(Bucket=bucket_name)['Contents']:
        #forDeletion = [{'Key':'02-03-2024.csv'}]
        response = rs.delete_objects(Bucket=bucket_name, Delete={'Objects':forDeletion})

    finish_pipeline = DummyOperator(task_id='finish-pipeline', dag=dag)
         
    trigger_child_dag = TriggerDagRunOperator(
        task_id='trigger_child_dag',
        trigger_dag_id='ins_temp'
        #execution_date="{{ execution_date }}"
    )
  
    
   # @task(task_id="insetr_table")
   # def ins_csv (str_data):
       
        
       
   # run_get = ins_csv(DATA)
    run_upload = pull_csv(data)
    run_del=del_csv(forDeletion,rs,BUCKET_NAME)
 
    run_upload >> [run_del, trigger_child_dag] >> finish_pipeline
    #run_upload