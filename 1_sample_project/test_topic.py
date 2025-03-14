from confluent_kafka.admin import AdminClient, NewTopic

admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})

topic_name = 'testtopic'
partitions = 3

new_topic = NewTopic(topic=topic_name, num_partitions=partitions)

# Create topic and wait for the result
futures = admin_client.create_topics([new_topic])

for topic, future in futures.items():
    try:
        future.result()  # Blocks until the topic is created or an error is raised
        print(f"Topic '{topic}' created successfully with {partitions} partitions")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {e}")
