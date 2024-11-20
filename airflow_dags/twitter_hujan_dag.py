from datetime import timedelta, datetime
import pytz
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import tweepy
import pandas as pd
import logging
from sqlalchemy import create_engine, Column, Float, String, DateTime, Integer, TEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
import psycopg2
import requests
import csv
from io import StringIO
from tweet_hujan import run_twitter_extract, run_twitter_transform, run_twitter_load

# Set default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('Asia/Jakarta')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG instance
dag_twitter = DAG(
    'twitter_hujan_dag',
    default_args=default_args,
    description='Tweets Hujan ETL DAG',
    schedule_interval='@daily',
)

# Create the PythonOperator for running the Extract task
run_extract = PythonOperator(
    task_id='complete_twitter_extract',
    python_callable=run_twitter_extract,
    dag=dag_twitter
)

# Create the PythonOperator for running the Transform task
run_transform = PythonOperator(
    task_id='complete_twitter_transform',
    python_callable=run_twitter_transform,
    dag=dag_twitter
)

# Create the PythonOperator for running the Load task
run_load = PythonOperator(
    task_id='complete_twitter_load',
    python_callable=run_twitter_load,
    dag=dag_twitter
)

run_extract >> run_transform >> run_load
