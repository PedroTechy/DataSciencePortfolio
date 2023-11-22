#import sys
#sys.path.insert(0, "/home/pedro/anaconda3/envs/airflow-env/lib/python3.9/site-packages")

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

import requests
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, inspect 


def read_config_file(file_path = 'config.txt'):
    try:
        with open(file_path, 'r') as file:
            # Read each line from the file and remove newline characters
            lines = [line.strip() for line in file.readlines()]
            return lines
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    
def extract_transform_load_weather_data():

    api_key, db_user, db_pass, db_host, db_name, db_table =  read_config_file('config.txt')

    db_properties = [db_user, db_pass, db_host, db_name, db_table]

    #city = 'Porto'
    city = ['Porto', 'Lisbon', 'Coimbra', 'Aveiro', 'Braga']
    
    if type(city) is list:
        for c in city:
            data = data_extraction(api_key, c) 
            data_loading(db_properties[:-1], data, db_properties[-1])
    else:
        data = data_extraction(api_key, city) 
        data_loading(db_properties[:-1], data, db_properties[-1])

    print("Data Loaded with success!")


def data_loading(db_properties, data_to_query, table_name):

    db_username, db_password, db_host, db_name = db_properties

    # Create a SQLAlchemy engine to connect to the MySQL database
    engine = create_engine(f'mysql+mysqlconnector://{db_username}:{db_password}@{db_host}/{db_name}')

    df = pd.DataFrame(data_to_query, index=[0])

    # Create a MetaData object
    metadata = MetaData()

    # Use the inspect function to check if the table exists
    inspector = inspect(engine)
    
    # If the table doesn't exist, create it
    if not inspector.has_table(table_name):
        metadata.create_all(bind=engine)
        df.to_sql(table_name, con=engine, index=False, if_exists='replace', method='multi')

    else:
        # Insert the DataFrame into the existing table
        df.to_sql(table_name, con=engine, if_exists='append', index=False, method='multi')


def data_extraction(api_key, city):

    base_url = "http://api.openweathermap.org/data/2.5/weather?"
    url = base_url + "appid=" + api_key + '&q=' + city

    response = requests.get(url).json()

    time_info = datetime.utcfromtimestamp(response['dt'])
    
    year = time_info.year
    month = time_info.month
    day = time_info.day
    hour = time_info.hour
    minute = time_info.minute

    weather_data = {
        'timestamp' : (time_info).strftime('%Y-%m-%d %H:%M:%S'),
        'year': year,
        'month': month,
        'day': day,
        'hour': hour,
        'minute': minute,
        'city': city,
        'temp': round(response['main']['temp'] - 273.15, 2),
        'temp_feels_like': round(response['main']['feels_like'] - 273.15, 2),
        'temp_min': round(response['main']['temp_min'] - 273.15, 2),
        'temp_max': round(response['main']['temp_max'] - 273.15, 2),
        'pressure': round(response['main']['pressure'], 2),
        'humidity': round(response['main']['humidity'], 2),
        'description': response['weather'][0]['description'],
        'icon': response['weather'][0]['icon'],
        'country' : response['sys']['country']
    }

    return weather_data

default_args = {
    'owner': 'pedro',
    'depends_on_past': False,
    'start_date': datetime(2023,9,9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_elt_flow',
    default_args=default_args,
    catchup=False, 
    description='A simple Airflow DAG for weather data extraction, transforming and loading',
    schedule_interval=timedelta(minutes=30), #do it every hour /m8 wanna use @hourly, check documentation
)

run_processing_task = PythonOperator(
    task_id='run_data_processing',
    python_callable=extract_transform_load_weather_data,
    dag=dag,
)
