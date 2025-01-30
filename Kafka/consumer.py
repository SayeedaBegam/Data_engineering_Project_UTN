from confluent_kafka import Consumer, Producer
import json

# KAFKA SettiNGS AND topics
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
TOPIC_RAW_DATA = 'parquet_stream'  # Kafka topic name
TOPIC_CLEAN_DATA = 'clean_data'
TOPIC_QUERY_METRICS = 'query_metrics'  # Kafka topic name for sorted durations
TOPIC_COMPILE_METRICS = 'compile_metrics'  # Kafka topic name for sorted durations
TOPIC_LEADERBOARD= 'leaderboard'
TOPIC_STRESS_INDEX = 'stressindex'

def create_consumer(topic, group_id):
    """Create a Confluent Kafka Consumer."""
    consumer_t = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Start reading from the beginning
        'enable.auto.commit': True       # Automatically commit offsets
    }, logger=None)
    consumer_t.subscribe([topic])
    return consumer_t

def main():
    consumer_raw_data = create_consumer(TOPIC_RAW_DATA, 'raw_data')
    consumer_leaderboard = create_consumer(TOPIC_LEADERBOARD, 'live_analytics')
    consumer_query_counter = create_consumer(TOPIC_QUERY_METRICS, 'live_analytics')
    consumer_compile = create_consumer(TOPIC_COMPILE_METRICS, 'live_analytics')
    consumer_stress = create_consumer(TOPIC_STRESS_INDEX, 'live_analytics')


    producer = Producer({
        'bootstrap.servers': KAFKA_BROKER
    })

    print(f"Listening for messages on topic '{TOPIC_RAW_DATA}'...")

    try:
        while True:
            raw_msg = consumer_raw_data.poll(timeout=1.0)
            leader_msg = consumer_leaderboard.poll(timeout=1.0)
            query_msg = consumer_query_counter.poll(timeout=1.0)
            compile_msg = consumer_compile.poll(timeout=1.0)
            stress_msg = consumer_stress.poll(timeout=1.0)

            if raw_msg is not None and not raw_msg.error():
                message_value = json.loads(raw_msg.value().decode('utf-8'))
                #print(f"Received from {TOPIC_RAW_DATA}: {message_value}")

            if leader_msg is not None and not leader_msg.error():
                message_value = json.loads(leader_msg.value().decode('utf-8'))
                print(f"Received from {TOPIC_LEADERBOARD}: {message_value}")
                print("Next message \n\n")

            if query_msg is not None and not query_msg.error():
                message_value = json.loads(query_msg.value().decode('utf-8'))
                print(f"Received from {TOPIC_QUERY_METRICS}: {message_value}")
                print("Next message \n\n")

            if compile_msg is not None and not compile_msg.error():
                message_value = json.loads(compile_msg.value().decode('utf-8'))
                print(f"Received from {TOPIC_COMPILE_METRICS}: {message_value}")
                print("Next message \n\n")

            if stress_msg is not None and not stress_msg.error():
                message_value = json.loads(stress_msg.value().decode('utf-8'))
                print(f"Received from {TOPIC_STRESS_INDEX}: {message_value}")
                print("Next message \n\n")



    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer_raw_data.close()
        consumer_leaderboard.close()
        consumer_query_counter.close()
        consumer_compile.close()
        consumer_stress.close()
        producer.flush()

if __name__ == '__main__':
    main()
