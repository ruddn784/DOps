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
from psycopg2 import Error
from io import StringIO
from airflow.utils import dates
import pandas as pd
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "ins_temp"

temp=''

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2023, 9, 5),
    schedule_interval=None,
    #schedule="@daily",
    #schedule=timedelta(days=1),
    #catchup=False,
) as dag:
    
    def filter_df(df):
        # Находим строки, в которых нет пустых значений
        mask = df.apply(lambda row: row.str.strip() != '').all(axis=1)

        # Фильтруем и возвращаем соответствующие записи
        filtered_df = df[mask]

        return filtered_df

    
    start_pipeline = DummyOperator(task_id='start-pipeline', dag=dag)
   
    @task(task_id=f"ins_{temp}")
    def ins_temp (temp, header_ls, filed_name, table_name):
        
        conn = psycopg2.connect("host=rc1b-500h1nnsu0agprlc.mdb.yandexcloud.net dbname=nds user=user1 password=db1user1 port=6432")
        cur = conn.cursor()
       
        try:
            query = '''SELECT {filed} FROM stage  '''.format(filed=filed_name)
           
            cur.execute(query)
            data = pd.DataFrame(cur.fetchall())
            df=data.set_axis(header_ls,axis = 1)
            filtered_df = filter_df(df)
            #=df[df['passenger_id']!='']
            df=filtered_df.drop_duplicates()
            f=df.to_csv(index=False, header=header_ls)
            temp_file = StringIO()
            temp_file.write(f)
            temp_file.seek(0)
           
            cur.copy_expert("COPY {table} FROM STDIN  WITH CSV HEADER".format(table=table_name), temp_file)
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
            query = '''select departure_airport  as airport_code, airpotr_departure  as airport_name,city_departure as city from stage 
                        union
                        select arrival_airport  as airport_code, airport_arrival  as airport_name, city_arrival city from stage'''
           
            cur.execute(query)
            data = pd.DataFrame(cur.fetchall())
            df=data.set_axis(['airport_code', 'airport_name', 'city'],axis = 1)
            filtered_df = filter_df(df)
           # #=df[df['passenger_id']!='']
            df=filtered_df.drop_duplicates()
            f=df.to_csv(index=False, header=['airport_code', 'airport_name', 'city'])
            temp_file = StringIO()
            temp_file.write(f)
            temp_file.seek(0)
           
            cur.copy_expert("COPY {table} FROM STDIN  WITH CSV HEADER".format(table='airport'), temp_file)
            conn.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
        finally:
            if conn:
                cur.close()
                conn.close()
                print("Соединение с PostgreSQL закрыто")
   
    @task(task_id=f"truncate_stage")
    def truncate_stage ():
        
        conn = psycopg2.connect("host=rc1b-500h1nnsu0agprlc.mdb.yandexcloud.net dbname=nds user=user1 password=db1user1 port=6432")
        cur = conn.cursor()
       
        try:
            query = '''truncate  table stage'''
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
        trigger_dag_id='ins_nda'
        #execution_date="{{ execution_date }}"
    )
  
    run_fligts = ins_temp ('flights', ['flight_id', 'flight_no', 'scheduled_departure', 'actual_departure', 'departure_airport', 'scheduled_arrival', 'actual_arrival', 'arrival_airport', 'status', 'aircraft_code'],
                           'flight_id, flight_no, scheduled_departure, actual_departure, departure_airport, scheduled_arrival, actual_arrival, arrival_airport, status, aircraft_code',
                           'flights')
                          
    run_pass = ins_temp ('pass',['passenger_id', 'passenger_name'], 'passenger_id, passenger_name', 'passenger')
    run_craft = ins_temp ('craft',['aircraft_code', 'model', 'seats_cnt'], 'aircraft_code, model, seats_cnt', 'aircraft')
    run_port = ins_port()
    run_ticket = ins_temp('ticket',['ticket_no','flight_id','passenger_id','fare_conditions','amount'],'ticket_no, flight_id, passenger_id,fare_conditions, amount','ticket_flight')
    run_trunc = truncate_stage ()
    
    start_pipeline >>[run_fligts,run_pass,run_craft,run_port,run_ticket] >> run_trunc >> finish_pipeline>>trigger_child_dag
    #start_pipeline >>[run_fligts,run_pass,run_craft,run_port,run_ticket] >>  finish_pipeline