import json
import time
from datetime import datetime
from kafka import KafkaConsumer


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

if __name__ == '__main__':
    consumer = setup_consumer()

    while consumer == 'not-ready':
        print('Brokers not availbe yet')
        time.sleep(5)
        consumer = setup_consumer()

    print('Waiting for messages...')
    for msg in consumer:
        print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] New message from Topic2 => ', msg.value)