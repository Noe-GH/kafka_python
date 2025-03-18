from confluent_kafka import Consumer

topic_name = 'testtopic'
consumer_group = 'testgroup'
# For assigning consumer to a particular partition
#partition_id = 1

c = Consumer({'bootstrap.servers': 'localhost:9092',
              'group.id': consumer_group,
              'auto.offset.reset': 'earliest'})

c.subscribe([topic_name])

# Assigning consumer to a particular partition
#c.assign([TopicPartition(topic_name, partition_id)])

try:
    while True:
        # Polling every second
        msg = c.poll(1.0)
        if msg is None:
            print("No messages. Listening")
            continue
        print(f'Message: {msg.value().decode("utf-8")}')
        print(f'Key: {msg.key().decode("utf-8")}')
        print(f'Partition: {msg.partition()}')
        print(f'Offset: {msg.offset()}')

except KeyboardInterrupt:
    pass
finally:
    c.close()

