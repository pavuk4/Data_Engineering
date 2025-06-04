import os
import csv
import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from minio import Minio, S3Error


def setup_consumer():
    try:
        consumer = KafkaConsumer(
            'Topic2',
            bootstrap_servers=['kafka1:29092', 'kafka2:29093'],
            group_id=None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        return consumer
    except Exception as e:
        if e == 'NoBrokersAvailable':
            print('Waiting for brokers to become available')
        
        return 'not-ready'

def upload_to_minio(minio_client, csv_file_path):
    print(f'Uploading {csv_file_path}...')

    minio_client.fput_object(
        'trips-bucket2', os.path.basename(csv_file_path), csv_file_path,
    )

    print(f'File {csv_file_path} was uploaded to MinIO.')

def open_file(month_year, init_data):
    print(f'Opening {month_year}...')
    
    path = os.path.join('./workdir', f'{month_year}.csv')
    month_year_file = open(path, 'a', newline='', encoding='utf-8')
    dict_writer = csv.DictWriter(month_year_file, fieldnames=init_data.keys())
    dict_writer.writeheader()
    dict_writer.writerow(init_data)
    month_year_file.flush()
    
    return month_year_file, dict_writer


if __name__ == '__main__':
    minio_client = Minio(
        'minio:9000',
        access_key='admin',
        secret_key='pass_admin',
        secure=False
    )
    consumer = setup_consumer()
    current_month = None
    csv_file = None
    csv_writer = None

    if not minio_client.bucket_exists('trips-bucket2'):
        minio_client.make_bucket('trips-bucket2')

    os.makedirs('./workdir', exist_ok=True)

    while consumer == 'not-ready':
        print('Brokers not availbe yet')
        time.sleep(5)
        consumer = setup_consumer()

    print('Waiting for messages...')
    for msg in consumer:
        data = msg.value
        dt = datetime.strptime(data['start_time'], '%Y-%m-%d %H:%M:%S')
        new_month = f'{dt.strftime("%B").lower()}_{dt.year}'

        if current_month is None:
            print(f'New month year: {new_month}')

            current_month = new_month
            csv_file, csv_writer = open_file(new_month, data)
        elif current_month == new_month:
            csv_writer.writerow(data)
            csv_file.flush()
        else:
            previous_csv = os.path.join('./workdir', f'{current_month}.csv')
            current_month = new_month
            csv_file.close()

            csv_file, csv_writer = open_file(new_month, data)

            try:
                upload_to_minio(minio_client, previous_csv)
                os.remove(previous_csv)
            except Exception as e:
                print(f'Error during upload file {previous_csv} to MinIO: {e}')
