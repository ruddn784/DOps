from __future__ import annotations
from airflow import DAG
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.models import Variable
import os
import datetime
import psycopg2
from psycopg2 import Error
from airflow.utils import dates
from airflow.operators.dummy_operator import DummyOperator
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "ins_dds"
query=Variable.get('ins_fact')

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2023, 9, 5),
    schedule_interval=None,
    #schedule="@daily",
    #schedule=timedelta(days=1),
    #catchup=False,
) as dag:
    
    start_pipeline = DummyOperator(task_id='start-pipeline', dag=dag)
   
    @task(task_id=f"ins_craft")
    def ins_craft ():
        
        conn = psycopg2.connect("host=rc1b-500h1nnsu0agprlc.mdb.yandexcloud.net dbname=nds user=user1 password=db1user1 port=6432")
        cur = conn.cursor()
       
        try:
           upsert_query = """
            INSERT INTO dds.dim_aircrafts(
            aircraft_code,  model,  seats_cnt)
        SELECT 
            aircraft_code, model, seats_cnt
        FROM
            nda.aircraft 
        ON CONFLICT DO NOTHING;
            """
        # Выполнение запроса
           cur.execute(upsert_query)
           conn.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
        finally:
            if conn:
                cur.close()
                conn.close()
                print("Соединение с PostgreSQL закрыто")

    @task(task_id=f"ins_pass")
    def ins_pass ():
        
        conn = psycopg2.connect("host=rc1b-500h1nnsu0agprlc.mdb.yandexcloud.net dbname=nds user=user1 password=db1user1 port=6432")
        cur = conn.cursor()
       
        try:
           upsert_query = """
            INSERT INTO dds.Dim_Passengers(
            passenger_id,  passenger_name)
        SELECT 
            passenger_id,  passenger_name
        FROM
            nda.passenger 
        ON CONFLICT DO NOTHING;
            """
        # Выполнение запроса
           cur.execute(upsert_query)
           conn.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
        finally:
            if conn:
                cur.close()
                conn.close()
                print("Соединение с PostgreSQL закрыто")
   
    @task(task_id=f"ins_port")
    def ins_port ():
        
        conn = psycopg2.connect("host=rc1b-500h1nnsu0agprlc.mdb.yandexcloud.net dbname=nds user=user1 password=db1user1 port=6432")
        cur = conn.cursor()
       
        try:
           upsert_query = """
            INSERT INTO dds.Dim_Airports(
            airport_code,  airport_name, city)
        SELECT 
            airport_code,  airport_name, city
        FROM
            nda.airport 
        ON CONFLICT DO NOTHING;
            """
        # Выполнение запроса
           cur.execute(upsert_query)
           conn.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
        finally:
            if conn:
                cur.close()
                conn.close()
                print("Соединение с PostgreSQL закрыто")
   
    @task(task_id=f"ins_tarif")
    def ins_tarif ():
        
        conn = psycopg2.connect("host=rc1b-500h1nnsu0agprlc.mdb.yandexcloud.net dbname=nds user=user1 password=db1user1 port=6432")
        cur = conn.cursor()
       
        try:
           upsert_query = """
            INSERT INTO dds.Dim_Tariff(
            fare_conditions)
        SELECT 
           fare_conditions
        FROM
            nda.ticket_flight 
        ON CONFLICT DO NOTHING;
            """
        # Выполнение запроса
           cur.execute(upsert_query)
           conn.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
        finally:
            if conn:
                cur.close()
                conn.close()
                print("Соединение с PostgreSQL закрыто")
   
     
   

    @task(task_id="ins_fact_flight")
    def ins_fact_flight (query):
        
        conn = psycopg2.connect("host=rc1b-500h1nnsu0agprlc.mdb.yandexcloud.net dbname=nds user=user1 password=db1user1 port=6432")
        cur = conn.cursor()
       
        try:
           upsert_query =query
        # Выполнение запроса
           cur.execute(upsert_query)
           conn.commit()
        except psycopg2.DatabaseError as error:
           print(f"Database error: {error}")
        finally:
            if conn:
                cur.close()
                conn.close()
                print("Соединение с PostgreSQL закрыто")

    finish_pipeline = DummyOperator(task_id='finish-pipeline', dag=dag)
  
    run_fligts = ins_fact_flight (query)
    #run_fligts = ins_fact_flight (query.format(d1='2015-10-29 10:05:00.000'))                      
    run_pass = ins_pass ()
    run_craft = ins_craft ()
    run_port = ins_port()
    run_ticket = ins_tarif()
    
start_pipeline >>[run_pass,run_craft,run_port,run_ticket] >>run_fligts >> finish_pipeline
