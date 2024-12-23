# -*- coding: utf-8 -*-
"""Tweets Pulling.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1NmTDpGJKwr8iOEKpNUEqFntyDDpVPOQs

# Keyword lebih banyak dar barbara
"""

pip install git+https://github.com/tweepy/tweepy.git

def fetch_tweets_multiple_keywords(bearer_token, keywords, max_results=100):
    import tweepy
    import pandas as pd

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
            return tweets_df
        else:
            print("No tweets found")
            return None

    except tweepy.TweepyException as e:
        print(f"An error occurred: {e}")
        return None


# Contoh penggunaan
bearer_token ="YOUR_BEARER_TOKEN"
keywords = ["YOUR_KEYWORDS"]
tweets_df = fetch_tweets_multiple_keywords(bearer_token, keywords)

if tweets_df is not None:
    print(tweets_df)

from google.colab import files
# Simpan DataFrame ke file CSV
tweets_df.to_csv("FILE_NAME.csv", index=False)

# Download file CSV
files.download("FILE_NAME.csv")