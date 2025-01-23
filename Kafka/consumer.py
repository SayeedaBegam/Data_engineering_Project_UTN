from kafka import KafkaConsumer
import json

# Kafka settings
KAFKA_BROKER = 'localhost:9092'  # Address of the Kafka broker
TOPIC_NAME = 'parquet-stream'    # Kafka topic name


def main():
    # Kafka Consumer settings
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',  # Read data from the beginning
        enable_auto_commit=True,       # Automatically commit offsets
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize in JSON format
    )

    print(f"Listening for messages on topic '{TOPIC_NAME}'...")

    try:
        for message in consumer:
            print(f"Received message: {message.value}")
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
