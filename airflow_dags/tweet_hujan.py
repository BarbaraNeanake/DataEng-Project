from datetime import datetime
import tweepy
import pandas as pd
import pytz
import logging
from sqlalchemy import create_engine, Column, Float, String, DateTime, Integer, TEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
import psycopg2
import requests
import csv
from io import StringIO

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# Function for Twitter extraction
def run_twitter_extract(**kwargs):
    logging.info("Extract tweeter_hujan task is starting")
    bearer_token = "AAAAAAAAAAAAAAAAAAAAAIw9xAEAAAAATrLUnds9%2F94LcRb%2FK%2FD%2FdhTGrRc%3D7QN4O0muIQpOhmNTFqFiIKcMjQta6sFQit1QF2ycWfnqf1q896"
    keywords = ["Jogja hujan", "Jogja Hujan", "jogja hujan", "jogja Hujan", "Jogja Ujan", "jogja ujan", "Jogja ujan", "jogja Ujan"]
    max_results = 100

    # Initialize the Twitter client
    client = tweepy.Client(bearer_token=bearer_token)

    # Combine keywords with OR operator
    search_query = " OR ".join(keywords) + " -is:reply -is:retweet -has:links lang:en"

    try:
        # Fetch tweets
        response = client.search_recent_tweets(
            query=search_query,
            tweet_fields=["author_id", "created_at", "public_metrics", "source", "text"],
            max_results=max_results,
        )

        tweets = response.data
        if tweets:
            attributes_container = [
                [
                    tweet.author_id,
                    tweet.created_at,
                    tweet.public_metrics["like_count"],
                    tweet.public_metrics["reply_count"],
                    tweet.source,
                    tweet.text,
                ]
                for tweet in tweets
            ]

            columns = ["Author ID", "Date Created", "Number of Likes", "Number of Replies", "Source of Tweet", "Tweet"]
            tweets_df = pd.DataFrame(attributes_container, columns=columns)

            # Get current date for file naming
            execution_date = kwargs['execution_date'].strftime("%Y-%m-%d_%H-%M-%S")
            task_instance_str = kwargs['ti'].task_id
            output_file_path = f'/home/WeatherWhispers/Output/twitter_hujan_{execution_date}_{task_instance_str}.csv'

            # Save the DataFrame to CSV
            tweets_df.to_csv(output_file_path, index=False)

            kwargs['ti'].xcom_push(key='extracted_file_path', value=output_file_path)
            logging.info(f"Data saved to {output_file_path}")

            # return tweets_df
        else:
            logging.warning("No tweets found")
            # return None

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise

    #except tweepy.TweepyException as e:
        #logging.error(f"Error in API request: {e}")
        #return None
    #except Exception as e:
        #logging.error(f"An unexpected error occurred: {e}")
        #return None

# Function for transforming Twitter data
def run_twitter_transform(**kwargs):
    logging.info("Transform task is starting")

    # Retrieve file path from XCom pushed by extract task
    input_file_path = kwargs['ti'].xcom_pull(task_ids='complete_twitter_extract', key='extracted_file_path')

    # Read data from the CSV
    twitter_data = pd.read_csv(input_file_path)

    # Convert 'Date Created' to datetime and localize timezone
    twitter_data['Date Created'] = pd.to_datetime(twitter_data['Date Created'], errors='coerce')
    twitter_data['timestamp_gmt7'] = twitter_data['Date Created'].dt.tz_localize('UTC').dt.tz_convert('Asia/Jakarta')
    twitter_data['timestamp_gmt7'] = twitter_data['timestamp_gmt7'].dt.tz_localize(None)

    # Dropping unnecessary columns
    twitter_data = twitter_data.drop(columns=['Author ID', 'Source of Tweet', 'Date Created'])

    # Dynamically generate the output file path
    execution_date = kwargs['execution_date'].strftime("%Y-%m-%d_%H-%M-%S")
    task_instance_str = kwargs['ti'].task_id
    output_file_path = f'/home/WeatherWhispers/Output/transformed_twitter_hujan_{execution_date}_{task_instance_str}.csv'

    # Save the transformed data
    twitter_data.to_csv(output_file_path, index=False)

    kwargs['ti'].xcom_push(key='transformed_file_path', value=output_file_path)
    logging.info(f"Transform task is done. Data saved to {output_file_path}")

# Function for loading Twitter data into PostgreSQL
def run_twitter_load(**kwargs):
    logging.info("Load task is starting")

    # Retrieve the transformed file path from XCom
    input_file_path = kwargs['ti'].xcom_pull(task_ids='complete_twitter_transform', key='transformed_file_path')

    twitter_data = pd.read_csv(input_file_path)

    # Connect to PostgreSQL
    engine = create_engine('postgresql://postgres:Weather.Whispers@rekdat-postgresql.cpm48umoy5cj.ap-southeast-2.rds.amazonaws.com:5432/FinalProject_Rekdat')

    Session = sessionmaker(bind=engine)
    session = Session()

    # Define the table
    Base = declarative_base()

    class TweetData(Base):
        _tablename_ = 'tweet_hujan_data'

        id = Column(Integer, primary_key=True, autoincrement=True)
        timestamp = Column(DateTime, nullable=False)
        likes = Column(Integer)
        replies = Column(Integer)
        tweet = Column(TEXT)

    # Check if the table exists, if not, create it
    inspector = inspect(engine)
    if not inspector.has_table('tweet_data'):
        Base.metadata.create_all(engine)

    try:
        # Insert data into the table
        for index, row in twitter_data.iterrows():
            tweet = TweetData(
                timestamp=row['timestamp_gmt7'],
                likes=row['Number of Likes'],
                replies=row['Number of Replies'],
                tweet=row['Tweet']
            )
            session.add(tweet)

        session.commit()
        logging.info("Data successfully loaded into PostgreSQL")

    except Exception as e:
        logging.error(f"Error loading data into PostgreSQL: {e}")
        session.rollback()
        raise

    finally:
        session.close()