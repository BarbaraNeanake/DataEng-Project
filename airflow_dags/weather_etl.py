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

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,  # Level log yang akan ditampilkan (INFO, DEBUG, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Format pesan log
    datefmt="%Y-%m-%d %H:%M:%S"  # Format waktu dalam log
)

# Define weather extraction function
def extract_weather(**kwargs):
    logging.info("Extract weather task is starting")

    try:
        # Get the current date
        current_date_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        # Create the URL with dynamic startDateTime and endDateTime values
        url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/yogyakarta/2024-11-16T00:00:00/{current_date_time}?unitGroup=metric&include=hours&key=585XY69LYCJ87R4JXWUCHJXND&contentType=csv"

        # Make the request to the API
        response = requests.get(url)

        # Check if the request was successful
        response.raise_for_status()

        # Extract data from the API response
        csv_weather = response.text

        # Read data CSV from string
        weather_df = pd.read_csv(StringIO(csv_weather))

        # Dynamically generate the file path based on DAG run execution date and task instance
        execution_date = kwargs['execution_date'].strftime("%Y-%m-%d_%H-%M-%S")
        task_instance_str = kwargs['ti'].task_id
        output_file_path = f'/home/WeatherWhispers/Output/weather_{execution_date}_{task_instance_str}.csv'

        # Save data CSV to a file
        weather_df.to_csv(output_file_path, index=False)

        # Push the dynamically generated file path to XCom
        kwargs['ti'].xcom_push(key='extracted_file_path', value=output_file_path)

        logging.info("Extract task is done")

    except requests.RequestException as e:
        logging.error(f"Error in API request: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise

def transform_weather(**kwargs):
    logging.info("Transform task is starting")

    # Retrieve the file path from the XCom pushed by the extract task
    input_file_path = kwargs['ti'].xcom_pull(task_ids='extract_weather_data', key='extracted_file_path')

    # Read data from the extracted CSV file
    weather_data = pd.read_csv(input_file_path)

    # Convert 'datetime' to datetime format
    weather_data['datetime'] = pd.to_datetime(weather_data['datetime'], errors='coerce')

    # Handle any invalid datetime (which could be NaT after coercion)
    weather_data = weather_data.dropna(subset=['datetime'])  # Remove rows with invalid datetime values

    # Now perform timezone localization and conversion
    weather_data['datetime'] = weather_data['datetime'].dt.tz_localize('UTC').dt.tz_convert('Asia/Jakarta')
    weather_data['datetime'] = weather_data['datetime'].dt.tz_localize(None)  # Remove timezone info if not needed

    # Dropping Columns that are not needed
    weather_data = weather_data.drop(['name',
                                      'feelslike', 
                                      'preciptype',
                                      'snow', 
                                      'snowdepth', 
                                      'windgust', 
                                      'winddir', 
                                      'sealevelpressure', 
                                      'visibility', 
                                      'solarradiation', 
                                      'solarenergy', 
                                      'uvindex', 
                                      'severerisk', 
                                      'icon', 
                                      'stations'], axis=1, errors='ignore')

    # Dynamically generate the file path based on DAG run execution date and task instance
    execution_date = kwargs['execution_date'].strftime("%Y-%m-%d_%H-%M-%S")
    task_instance_str = kwargs['ti'].task_id
    output_file_path = f'/home/WeatherWhispers/Output/transformed_weather_{execution_date}_{task_instance_str}.csv'

    # Save the transformed data to a new CSV file
    weather_data.to_csv(output_file_path, index=False)

    # Push the dynamically generated file path to XCom
    kwargs['ti'].xcom_push(key='transformed_file_path', value=output_file_path)

    logging.info("Transform task is done")

# Define your load function
def load_data_into_postgres(**kwargs):
    logging.info("Load task is starting")

    # Retrieve the file path from the XCom pushed by the transform task
    input_file_path = kwargs['ti'].xcom_pull(task_ids='transform_weather_data', key='transformed_file_path')

    # Read data from the transformed CSV file
    data = pd.read_csv(input_file_path)

    # Connect to the PostgreSQL database using SQLAlchemy
    engine = create_engine('postgresql://postgres:Weather.Whispers@rekdat-postgresql.cpm48umoy5cj.ap-southeast-2.rds.amazonaws.com:5432/FinalProject_Rekdat')

    # Create a SQLAlchemy session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Define the table outside the if-else block
    Base = declarative_base()

    class WeatherData(Base):
        __tablename__ = 'weather_data'

        id = Column(Integer, primary_key=True, autoincrement=True)
        datetime = Column(DateTime, nullable=False)
        temperature = Column(Float)
        dew_point = Column(Float)
        humidity = Column(Float)
        precipitation = Column(Float)
        precipitation_prob = Column(Float)
        wind_speed = Column(Float)
        cloud_cover = Column(Float)
        conditions = Column(String(255))
        
    # Check if the table exists
    inspector = inspect(engine)
    if not inspector.has_table('weather_data'):
        # If the table does not exist, create it
        Base.metadata.create_all(engine)
    else:
        # If the table exists, delete all rows
        session.query(WeatherData).delete()

    try:
        # Insert data into the 'weather_data' table
        for index, row in data.iterrows():
            weather_data = WeatherData(
                datetime=row['datetime'],
                temperature=row['temp'],
                dew_point=row['dew'],
                humidity=row['humidity'],
                precipitation=row['precip'],
                precipitation_prob=row['precipprob'],
                wind_speed=row['windspeed'],
                cloud_cover=row['cloudcover'],
                conditions=row['conditions'],
            )
            session.add(weather_data)

        # Commit the changes
        session.commit()

        logging.info("Load task is done")

    except Exception as e:
        logging.error(f"An error occurred while loading data into PostgreSQL: {e}")
        # Rollback the changes in case of an error
        session.rollback()
        raise

    finally:
        # Close the session
        session.close()