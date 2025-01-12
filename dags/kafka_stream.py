from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import time
import logging
from kafka import KafkaProducer
import requests
import pandas as pd  # Importing Pandas for data preprocessing

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 3, 10, 00),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to fetch data
def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    return res['results'][0]

# Function to format data
def format_data(res):
    data = {}
    location_id_value = hash(f"{res['location']['street']['number']} {res['location']['street']['name']} {res['location']['postcode']}")
    
    # user table
    data['user_id'] = res['login']['uuid']
    data['dob_id'] = hash(res['dob']['date'])
    data['registered_id'] = hash(res['registered']['date'])
    data['location_id'] = str(location_id_value)
    data['login_id'] = res['login']['uuid']
    data['contact_id'] = hash(res['phone'])
    data['gender'] = res['gender']
    data['nationality'] = res['nat']
    
    # location table
    data['street'] = f"{res['location']['street']['number']} {res['location']['street']['name']}"
    data['city'] = res['location']['city']
    data['state'] = res['location']['state']
    data['country'] = res['location']['country']
    data['postcode'] = res['location']['postcode']
    data['coordinates_latitude'] = res['location']['coordinates']['latitude']
    data['coordinates_longitude'] = res['location']['coordinates']['longitude']
    
    # contact table
    data['phone'] = res['phone']
    data['cell'] = res['cell']
    data['email'] = res['email']
    
    # login table
    data['username'] = res['login']['username']
    data['password'] = res['login']['password']
    data['salt'] = res['login']['salt']
    data['md5'] = res['login']['md5']
    data['sha1'] = res['login']['sha1']
    data['sha256'] = res['login']['sha256']
    
    # register table
    registered_date = datetime.strptime(res['registered']['date'], '%Y-%m-%dT%H:%M:%S.%fZ')
    data['registered_date'] = res['registered']['date']
    data['years_since_registration'] = datetime.now().year - registered_date.year
    
    # date of birth table
    data['dob'] = res['dob']['date']
    data['age'] = res['dob']['age']
    
    return data

# Enhanced preprocessing using Pandas
def preprocess_data(data):
    # Convert dictionary to DataFrame for easier validation
    df = pd.DataFrame([data])

    # Check for null or empty values
    if df.isnull().any().any():
        missing_cols = df.columns[df.isnull().any()].tolist()
        logging.warning(f"Dropping data due to missing fields: {missing_cols}")
        return None

    # Check for negative or invalid numerical values (e.g., age or coordinates)
    if (df['age'] < 0).any():
        logging.warning("Dropping data due to invalid age value")
        return None

    if not (-90 <= float(df['coordinates_latitude'].iloc[0]) <= 90) or not (-180 <= float(df['coordinates_longitude'].iloc[0]) <= 180):
        logging.warning("Dropping data due to invalid coordinates")
        return None

    # Additional validation logic can be added here (e.g., string length, valid email format, etc.)
    if not df['email'].str.contains("@").all():
        logging.warning("Dropping data due to invalid email format")
        return None

    # Return the cleaned data as a dictionary
    return df.to_dict(orient='records')[0]

# Function to stream data for one minute
def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    start_time = time.time()

    while time.time() - start_time < 60:  # Stream for 1 minute
        try:
            res = get_data()
            formatted_data = format_data(res)
            preprocessed_data = preprocess_data(formatted_data)
            
            if preprocessed_data:  # Send only if data is valid
                producer.send('users_created', json.dumps(preprocessed_data).encode('utf-8'))
            else:
                logging.warning("Data dropped during preprocessing")

            time.sleep(1)

        except Exception as e:
            logging.error(f"Error during streaming: {e}")

# Define the DAG
with DAG(
    'user_automation_with_preprocessing',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # Run every 1 minute
    catchup=False
) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data,
    )
