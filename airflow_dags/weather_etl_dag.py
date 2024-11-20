from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import requests
import csv
from io import StringIO
import logging
import pytz

from sqlalchemy import create_engine, Column, Float, String, DateTime, inspect, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from weather_etl import extract_weather, transform_weather, load_data_into_postgres

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('Asia/Jakarta')),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    'weather_etl_dag',
    default_args=default_args,
    description='A DAG for extracting, transforming, and loading weather data',
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    # Task to extract weather data
    extract_task = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather,
    )

    # Task to transform weather data
    transform_task = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather,
    )

    # Task to load weather data
    load_task = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_data_into_postgres,
    )

    # Set the task dependencies
    extract_task >> transform_task >> load_task
