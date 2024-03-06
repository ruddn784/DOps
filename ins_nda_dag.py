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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "ins_nda"

temp=''

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2023, 9, 5),
    schedule_interval=None,
    #schedule="@daily",
    #schedule=timedelta(days=1),
    #catchup=False,
) as dag:
    
    start_pipeline = DummyOperator(task_id='start-pipeline', dag=dag)
   
    @task(task_id=f"ins_{temp}")
    def ins_nda (temp, table_name):
        
        conn = psycopg2.connect("host=rc1b-500h1nnsu0agprlc.mdb.yandexcloud.net dbname=nds user=user1 password=db1user1 port=6432")
        cur = conn.cursor()
       
        try:
           upsert_query = """
           INSERT INTO nda.{table} 
           SELECT * FROM {table}
           ON CONFLICT DO NOTHING;
           """.format(table=table_name)
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

    @task(task_id="ins_flights")
    def ins_flights ():
        
        conn = psycopg2.connect("host=rc1b-500h1nnsu0agprlc.mdb.yandexcloud.net dbname=nds user=user1 password=db1user1 port=6432")
        cur = conn.cursor()
       
        try:
           upsert_query = """
        INSERT INTO nda.flights (
            flight_id,
            flight_no,
            scheduled_departure,
            actual_departure,
            departure_airport,
            scheduled_arrival,
            actual_arrival,
            arrival_airport,
            status,
            aircraft_code
        )
        SELECT
            CAST(flight_id AS integer),
            CAST(flight_no AS char(6)),
            TO_TIMESTAMP(scheduled_departure, 'YYYY-MM-DD HH24:MI:SSZZ'),
            TO_TIMESTAMP(actual_departure, 'YYYY-MM-DD HH24:MI:SSZZ'),
            departure_airport,
            TO_TIMESTAMP(scheduled_arrival, 'YYYY-MM-DD HH24:MI:SSZZ'),
            TO_TIMESTAMP(actual_arrival, 'YYYY-MM-DD HH24:MI:SSZZ'),
            arrival_airport,
            status,
            aircraft_code
        FROM
            public.flights
        ON CONFLICT (flight_id) DO UPDATE SET
            flight_no = EXCLUDED.flight_no,
            scheduled_departure = EXCLUDED.scheduled_departure,
            actual_departure = EXCLUDED.actual_departure,
            departure_airport = EXCLUDED.departure_airport,
            scheduled_arrival = EXCLUDED.scheduled_arrival,
            actual_arrival = EXCLUDED.actual_arrival,
            arrival_airport = EXCLUDED.arrival_airport,
            status = EXCLUDED.status,
            aircraft_code = EXCLUDED.aircraft_code;
        """
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

    @task(task_id="ins_aircraft")
    def ins_aircraft ():
        
        conn = psycopg2.connect("host=rc1b-500h1nnsu0agprlc.mdb.yandexcloud.net dbname=nds user=user1 password=db1user1 port=6432")
        cur = conn.cursor()
       
        try:
           upsert_query = """
        INSERT INTO nda.aircraft (
            aircraft_code,
            model,
            seats_cnt
            )
        SELECT 
            aircraft_code,
            model,
            CAST(seats_cnt AS integer)
        FROM
            public.aircraft
        ON CONFLICT DO NOTHING;
        """
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

    @task(task_id="ins_ticket_flight")
    def ins_ticket_flight ():
        
        conn = psycopg2.connect("host=rc1b-500h1nnsu0agprlc.mdb.yandexcloud.net dbname=nds user=user1 password=db1user1 port=6432")
        cur = conn.cursor()
       
        try:
           upsert_query = """
        INSERT INTO nda.ticket_flight (
            ticket_no,
            flight_id,
            passenger_id,
            fare_conditions,
            amount
            )
        SELECT 
            ticket_no,
            CAST(flight_id AS integer),
            passenger_id,
            fare_conditions,
            CAST(amount AS numeric)
        FROM
            public.ticket_flight
        ON CONFLICT DO NOTHING;
        """
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
    

    @task(task_id=f"truncate_temp")
    def truncate_temp ():
        
        conn = psycopg2.connect("host=rc1b-500h1nnsu0agprlc.mdb.yandexcloud.net dbname=nds user=user1 password=db1user1 port=6432")
        cur = conn.cursor()
       
        try:
            query = '''truncate  table flights, aircraft, airport, ticket_flight, passenger '''
            cur.execute(query)
            conn.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
        finally:
            if conn:
                cur.close()
                conn.close()
                print("Соединение с PostgreSQL закрыто")

    finish_pipeline = DummyOperator(task_id='finish-pipeline', dag=dag)

    trigger_child_dag = TriggerDagRunOperator(
        task_id='trigger_child_dag',
        trigger_dag_id='ins_dds'
        #execution_date="{{ execution_date }}"
    )
  
    run_fligts = ins_flights ()
                          
    run_pass = ins_nda ('passenger','passenger')
    run_craft = ins_aircraft ()
    run_port = ins_nda('airport','airport')
    run_ticket = ins_ticket_flight()
    run_trunc = truncate_temp ()
    
start_pipeline >>[run_pass,run_craft,run_port] >>run_fligts >> run_ticket >>run_trunc>> finish_pipeline>>trigger_child_dag
