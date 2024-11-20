from datetime import datetime
import tweepy
import pandas as pd
import pytz
import logging
from sqlalchemy import create_engine, Column, Integer, Float, DateTime, TEXT, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

def run_twitter_extract():
    """
    Fungsi untuk mengambil data tweet menggunakan API Twitter.
    """
    bearer_token = "AAAAAAAAAAAAAAAAAAAAAFk9xAEAAAAAFYtULiN1tMYeaiELmacX6%2BUxfrY%3DTXEeyTVp0ej3vcV9AkMUPonHYXF41mS8tzFn2YCXI4cpMFmbs6"
    keywords = ["Jogja panas", "Jogja Panas", "jogja panas", "jogja Panas", "Jogja Sumuk", "jogja sumuk", "Jogja sumuk", "jogja Sumuk"]
    max_results = 100
    
    # Inisialisasi Client
    client = tweepy.Client(bearer_token=bearer_token)

    # Gabungkan kata kunci dengan operator OR
    search_query = " OR ".join(keywords) + " -is:reply -is:retweet -has:links lang:en"

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
                    tweet.public_metrics["reply_count"],
                    tweet.source,
                    tweet.text,
                ]
                for tweet in tweets
            ]

            # Kolom DataFrame
            columns = ["Author ID", "Date Created", "Number of Likes", "Number of Replies", "Source of Tweet", "Tweet"]
            tweets_df = pd.DataFrame(attributes_container, columns=columns)
            
            # Dapatkan tanggal saat data diambil
            current_date = datetime.now().strftime("%Y%m%d")
            
            # Simpan ke CSV dengan nama file yang mengandung tanggal dan directory path
            output_file_path = f'/home/WeatherWhispers/Output/tweet_panas{current_date}.csv'
            tweets_df.to_csv(output_file_path, index=False)

            print(f"Data saved to {output_file_path}")
            return tweets_df
        else:
            print("No tweets found")
            return None

    except tweepy.TweepyException as e:
        print(f"An error occurred: {e}")
        return None


def run_twitter_transform(input_df):
    """
    Transformasi data Twitter.
    """
    # Konversi dari text ke datetime
    input_df['Date Created'] = pd.to_datetime(input_df['Date Created'], utc=True)
    
    # Konversi timezone ke GMT+7
    input_df['timestamp_gmt7'] = input_df['Date Created'].dt.tz_convert('Asia/Jakarta').dt.tz_localize(None)
    
    # Drop kolom yang tidak diperlukan
    input_df.drop(columns=['Author ID', 'Source of Tweet', 'Date Created'], inplace=True)

    # Simpan hasil transformasi ke file CSV dengan directory path
    transformed_file_path = f'/home/WeatherWhispers/Output/transformed_twitter_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    input_df.to_csv(transformed_file_path, index=False)
    return transformed_file_path


def run_twitterhujan_load(transformed_file_path):
    """
    Load data ke PostgreSQL.
    """
    logging.info("Load task is starting")

    # Baca data dari CSV
    data = pd.read_csv(transformed_file_path)

    # Koneksi ke database PostgreSQL
    engine = create_engine('postgresql://postgres:Weather.Whispers@rekdat-postgresql.cpm48umoy5cj.ap-southeast-2.rds.amazonaws.com:5432/FinalProject_Rekdat')
    Session = sessionmaker(bind=engine)
    session = Session()

    # Definisikan Base dan Model
    Base = declarative_base()

    class PanasData(Base):
        _tablename_ = 'panas_data'

        id = Column(Integer, primary_key=True, autoincrement=True)
        timestamp = Column(DateTime, nullable=False)
        likes = Column(Integer)
        replies = Column(Integer)
        tweet = Column(TEXT)

    # Buat tabel jika belum ada
    inspector = inspect(engine)
    if not inspector.has_table('panas_data'):
        Base.metadata.create_all(engine)
    else:
        # Jika tabel ada, hapus data lama
        session.query(PanasData).delete()

    try:
        # Masukkan data ke tabel
        for index, row in data.iterrows():
            panas_data = PanasData(
                timestamp=row['timestamp_gmt7'],
                likes=row['Number of Likes'],
                replies=row['Number of Replies'],
                tweet=row['Tweet'],
            )
            session.add(panas_data)

        # Commit perubahan
        session.commit()
        logging.info("Load task is done")

    except Exception as e:
        logging.error(f"An error occurred while loading data into PostgreSQL: {e}")
        session.rollback()
        raise

    finally:
        session.close()