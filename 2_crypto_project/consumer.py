from confluent_kafka import Consumer
import csv
import json
import time

topic_name = 'cryptodata'
c_group_id = 'groupcrypto'

consumer_config = {'bootstrap.servers': 'localhost:9092', 'group.id': c_group_id, 'auto.offset.reset': 'earliest'}

consumer = Consumer(consumer_config)
consumer.subscribe([topic_name])

csv_file_path = 'crypto_prices.csv'

with open(csv_file_path, 'a', newline='') as csv_file:

    writer = csv.DictWriter(csv_file, fieldnames=['timestamp', 'name', 'symbol', 'price'])

    # Header only if file doesn't exist
    if not csv_file.tell():
        writer.writeheader()

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            elif msg.error():
                print(f'Error {msg.error()}')
                continue

            row = json.loads(msg.value())
            writer.writerow(row)
            print(f'Data received and written in {csv_file_path}')
    
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print('Consumer closed')
        
        # Time to allow for the last messages to be received before closing the consumer.
        time.sleep(2)
