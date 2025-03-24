from confluent_kafka import Producer
import requests
import json
import time

cryptos_to_track = ['bitcoin', 'ethereum', 'ripple', 'littlecoin', 'cardano', 'polkadot', 'stellar', 'eos', 'tron', 'dogecoin']

api_url = 'https://api.coincap.io/v2/assets'

producer_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_config)
topic_name = 'cryptodata'

while True:
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()

    # Verify JSON response structure
    if 'data' in data and isinstance(data['data'], list):
        rows = [{'timestamp': int(time.time()), 'name': crypto['name'], 'symbol': crypto['symbol'], 'price': crypto['priceUsd']} for crypto in data['data'] if crypto['id'] in cryptos_to_track]

        for row in rows:
            producer.produce(topic_name, value=json.dumps(row))
        
        producer.flush()

        print(f'Data sent to Kafka topic {topic_name}')

    else:
        print(f'Error whith the API request. Status code: {response.status_code}')

    # API calls every 30 seconds
    time.sleep(30)

