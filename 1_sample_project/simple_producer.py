from confluent_kafka import Producer

topic_name = 'testtopic'
key = 'key1'
msg = 'Message 1'
partition_id = 1

p = Producer({'bootstrap.servers': 'localhost:9092'})

# Sending message to a particular partition
p.produce(topic_name, msg, partition=partition_id)

# To force buffered messages to be sent to the brokers before continuing execution
p.flush()

# Setting a particular key
p.produce(topic_name, msg, key=key)

p.flush()

