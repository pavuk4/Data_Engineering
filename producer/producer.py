import csv
import json
import time
from kafka import KafkaProducer


def setup_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka1:29092', 'kafka2:29093'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        return producer
    except Exception as e:
        if e == 'NoBrokersAvailable':
            print('Waiting for brokers to become available')
        
        return 'not-ready'

if __name__ == '__main__':
    producer = setup_producer()

    while producer == 'not-ready':
        print('Brokers not availbe yet')
        time.sleep(5)
        producer = setup_producer()

    with open('/data/Divvy_Trips_2019_Q4.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            print(f"Sending: {row}")
        
            producer.send('Topic1', row)
            producer.send('Topic2', row)
            
            time.sleep(1)  # затримка для демонстрації

    producer.flush()
    producer.close()