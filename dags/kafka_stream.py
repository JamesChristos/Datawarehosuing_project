from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 3, 10, 00),
}

def get_data():
    import requests
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res):
    data = {}

    location_id_value = hash(f"{res['location']['street']['number']} {res['location']['street']['name']} {res['location']['postcode']}")

    # user table
    data['user_id'] = res['login']['uuid']
    data['dob_id'] = hash(res['dob']['date'])
    data['registered_id'] = hash(res['registered']['date'])  # Ensure this is being assigned
    data['location_id'] = hash(f"{res['location']['street']['number']} {res['location']['street']['name']} {res['location']['postcode']}")
    data['location_id'] = str(location_id_value)
    data['login_id'] = res['login']['uuid']
    data['contact_id'] = hash(res['phone'])
    data['gender'] = res['gender']
    data['nationality'] = res['nat']

    # location table
    data['location_id'] = str(location_id_value) 
    data['street'] = f"{res['location']['street']['number']} {res['location']['street']['name']}"
    data['city'] = res['location']['city']
    data['state'] = res['location']['state']
    data['country'] = res['location']['country']
    data['postcode'] = res['location']['postcode']
    data['coordinates_latitude'] = res['location']['coordinates']['latitude']
    data['coordinates_longitude'] = res['location']['coordinates']['longitude']

    # contact table
    data['contact_id'] = hash(res['phone'])
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
    data['registered_date'] = res['registered']['date']
    registered_date = datetime.strptime(res['registered']['date'], '%Y-%m-%dT%H:%M:%S.%fZ')
    data['years_since_registration'] = datetime.now().year - registered_date.year

    # date of birth table
    data['dob'] = res['dob']['date']
    data['age'] = res['dob']['age']

    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    
    res = get_data()
    res = format_data(res)
    # print(json.dumps(res, indent=3))

    producer = KafkaProducer(bootstrap_servers=['broker:29092'],
                            max_block_ms=5000)
    
    curr_time = time.time()
    
    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('users_created', json.dumps(res).encode('utf-8'))

        except Exception as e:
            logging.error(f"Error: {e}")
            continue


with DAG('user_automation',
        default_args=default_args,
        schedule='@daily',
        catchup=False
        ) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data,
    )

# stream_data()