import tweepy
import pandas as pd
import files
from datetime import datetime, timedelta
import requests
import csv
from io import StringIO
import logging

from sqlalchemy import create_engine, Column, Float, String, DateTime, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime.utcnow(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate DAG
dag_twitter = DAG(
    '1_tweets_etl_dag',
    default_args=default_args,
    description='Tweets ETL DAG',
    schedule_interval='@hourly',
)

# Define your extraction function
def extract_data(**kwargs):
    logging.info("Extract task is starting")

    try:
        # API Key dan Token dari Developer Platform
        bearer_token = "AAAAAAAAAAAAAAAAAAAAACj0wgEAAAAAJz3JgQlNi3HsmKzrsGqI6D7xJW4%3DGqAz4vc5T3cVICdjhQcIZHNEbzmhZ4JdeB1lxH8ppH0eF7O9fP"

        # Inisialisasi Client
        client = tweepy.Client(bearer_token=bearer_token)

        # Query untuk pencarian
        search_query = "jogja hujan -is:retweet -is:reply -has:links lang:en"
        max_results = 100

        # Pencarian Tweet
        try:
            response = client.search_recent_tweets(
                query=search_query,
                tweet_fields=["author_id", "created_at", "public_metrics", "source", "text"],
                max_results=max_results,
            )

            # Ekstraksi data dari response
            tweets = response.data
            if tweets:
                attributes_container = [
                    [
                        tweet.author_id,
                        tweet.created_at,
                        tweet.public_metrics["like_count"],
                        tweet.source,
                        tweet.text,
                    ]
                    for tweet in tweets
                ]

                # Kolom DataFrame
                columns = ["Author ID", "Date Created", "Number of Likes", "Source of Tweet", "Tweet"]
                tweets_df = pd.DataFrame(attributes_container, columns=columns)
                print(tweets_df)
            else:
                print("No tweets found")

        except tweepy.TweepyException as e:
            print(f"An error occurred: {e}")

        tweets_df

        tweets_df.to_csv('tweets.csv')
        files.download('tweets.csv')
        
        # Get the current year and current date
        current_year = datetime.now().year
        current_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # Create the URL with dynamic startDateTime and endDateTime values
        url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history?&aggregateHours=24&startDateTime={current_year}-01-01T00:00:00&endDateTime={current_date}&unitGroup=metric&contentType=csv&dayStartTime=0:0:00&dayEndTime=0:0:00&location=US&key=9KVZ52XFSR3SZVFPKWAXWB3AS"

        # Make the request to the API
        response = requests.get(url)

        # Check if the request was successful
        response.raise_for_status()

        # Extract data from the API response
        csv_data = response.text

        # Read data CSV from string
        df = pd.read_csv(StringIO(csv_data))

        # Dynamically generate the file path based on DAG run execution date and task instance
        execution_date = kwargs['execution_date']
        task_instance_str = kwargs['ti'].task_id
        output_file_path = f'/opt/airflow/data/weather_{execution_date}_{task_instance_str}.csv'

        # Save data CSV to a file
        df.to_csv(output_file_path, index=False)

        # Push the dynamically generated file path to XCom
        kwargs['ti'].xcom_push(key='extracted_file_path', value=output_file_path)

        logging.info("Extract task is done")

    except requests.RequestException as e:
        logging.error(f"Error in API request: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise