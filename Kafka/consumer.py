from confluent_kafka import Consumer, Producer
import json
import ddb_wrappers as ddb
import duckdb
import time

DUCKDB_FILE = "data.duckdb"

# KAFKA SettiNGS AND topics
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
TOPIC_RAW_DATA = 'parquet_stream'  # Kafka topic name
TOPIC_CLEAN_DATA = 'clean_data'
TOPIC_QUERY_METRICS = 'query_metrics'  # Kafka topic name for sorted durations
TOPIC_COMPILE_METRICS = 'compile_metrics'  # Kafka topic name for sorted durations
TOPIC_LEADERBOARD= 'leaderboard'
TOPIC_STRESS_INDEX = 'stressindex'
LEADERBOARD_COLUMNS = ['instance_id','query_id','user_id','arrival_timestamp','compile_duration_ms']
QUERY_COLUMNS = ['instance_id','was_aborted','was_cached','query_type']
COMPILE_COLUMNS = ['instance_id','num_joins','num_scans','num_aggregations','mbytes_scanned', 'mbytes_spilled']
STRESS_COLUMNS = ['execution_duration_ms','mbytes_spilled']

## Expert Analytics
TOPIC_FLAT_TABLES = 'flattened'
FLAT_COLUMNS = ['instance_id','query_id','write_table_ids','read_table_ids','arrival_timestamp','query_type']


def initialize_duckdb():
    """Create a table in DuckDB if it does not exist"""
    con = duckdb.connect(DUCKDB_FILE)
    '''
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            instance_id BIGINT,
            cluster_size DOUBLE,
            user_id BIGINT,
            database_id BIGINT,
            query_id BIGINT,
            arrival_timestamp TIMESTAMP,
            compile_duration_ms DOUBLE,
            queue_duration_ms BIGINT,
            execution_duration_ms BIGINT,
            feature_fingerprint VARCHAR,
            was_aborted BOOLEAN,
            was_cached BOOLEAN,
            cache_source_query_id DOUBLE,
            query_type VARCHAR,
            num_permanent_tables_accessed DOUBLE,
            num_external_tables_accessed DOUBLE,
            num_system_tables_accessed DOUBLE,
            read_table_ids VARCHAR,
            write_table_ids DOUBLE,
            mbytes_scanned DOUBLE,
            mbytes_spilled DOUBLE,
            num_joins BIGINT,
            num_scans BIGINT,
            num_aggregations BIGINT,
            batch_id BIGINT,
        )
    """)
    '''
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS LIVE_QUERY_METRICS (
        instance_id BIGINT,
        was_aborted BOOLEAN,
        was_cached BOOLEAN,
        query_type VARCHAR
    )
    """)
    con.execute(f"""
            CREATE OR REPLACE TABLE flattened_table_ids(
            instance_id int32,
            query_id int64,
            write_table_ids int64,
            read_table_ids int64,
            arrival_timestamp timestamp,
            query_type varchar)
                """)
    con.close()


def create_consumer(topic, group_id):
    """Create a Confluent Kafka Consumer."""
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Read from the beginning if no offset found
        'enable.auto.commit': False,       # Enable automatic commit
        'enable.partition.eof': False,    # Avoid EOF issues
        })
    consumer.subscribe([topic])
    return consumer

def main():
    long_avg = 0
    short_avg = 0
    consumer_raw_data = create_consumer(TOPIC_RAW_DATA, 'raw_data')
    consumer_leaderboard = create_consumer(TOPIC_LEADERBOARD, 'live_analytics')
    consumer_query_counter = create_consumer(TOPIC_QUERY_METRICS, 'live_analytics')
    consumer_compile = create_consumer(TOPIC_COMPILE_METRICS, 'live_analytics')
    consumer_stress = create_consumer(TOPIC_FLAT_TABLES, 'live_analytics')
    initialize_duckdb()
    con = duckdb.connect(DUCKDB_FILE)
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
            #stress_index = calculate_stress(consumer_stress,long_avg,short_avg)
            #long_avg = stress_index[1]
            #short_avg = stress_index[0]
            #print(stress_index)
            ddb.parquet_to_table(consumer_stress,'flattened_table_ids',con, FLAT_COLUMNS,TOPIC_FLAT_TABLES)
            #time.sleep(5)
            ddb.check_duckdb_table('flattened_table_ids',con)
            if stress_msg is not None and not stress_msg.error():
                message_value = json.loads(stress_msg.value().decode('utf-8'))
                print(f"Received from {TOPIC_FLAT_TABLES}: {message_value}")
            '''
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
            '''
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

