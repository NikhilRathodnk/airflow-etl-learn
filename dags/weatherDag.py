from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.get_weather import get_weather
from src.transform_data import transform_data
from src.load_table import load_table
import os

# ✅ Print log for DAG initialization
print("✅ DAG 'nikhil_weather_etl' initialized and ready to fetch weather data.")

# Define the default dag arguments.
default_args = {
    'owner': 'Nikhil Kumar Rathod',
    'depends_on_past': False,
    'email': ['nikhilxr6@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 1, 1),
}

# ✅ Define the DAG with custom ID and new schedule
dag = DAG(
    dag_id='nikhil_weather_etl',
    default_args=default_args,
    description='Customized ETL DAG to fetch and process weather data',
    schedule_interval='*/15 * * * *',  # Runs every 15 minutes
    catchup=False,
    tags=['weather', 'ETL', 'custom'],
)

# Define the tasks
t1 = PythonOperator(
    task_id='get_weather_data',
    python_callable=get_weather,
    dag=dag
)

t2 = PythonOperator(
    task_id='transform_weather_data',
    python_callable=transform_data,
    dag=dag
)

t3 = PythonOperator(
    task_id='load_weather_data',
    python_callable=load_table,
    dag=dag
)

# Task dependencies
t1 >> t2 >> t3

