    # Creates a table to store the flattened write and read table ids
    # Here the Kafka topic will write into.
    create_flattened_table_ids_table = """
        CREATE OR REPLACE TABLE flattened_table_ids(
            instance_id int32,
            query_id int64,
            write_table_id int64,
            read_table_id int64,
            arrival_timestamp timestamp,
            query_type varchar)
      """
     # Create flattend read and write table ids table
    con.execute(create_flattened_table_ids_table)


# Creates a table to store the times between ingestion per write_table_id
# this table will be filled as an intermediate step in the analytics workflow
    create_ingestion_intervals_per_table = """
        CREATE OR REPLACE TABLE ingestion_intervals_per_table (
                   instance_id int32,
                   query_id int64,
                   write_table_id int64,
                   current_timestamp timestamp,
                   next_timestamp timestamp)
    """
    # Create ingestion intervals table
    con.execute(create_ingestion_intervals_per_table)

# creates a table to store the output 
# final intermediate table in analytics workflow
  create_output_table = """
        CREATE OR REPLACE TABLE output_table(
           instance_id int32,
           query_id int64,
           query_type varchar,
           write_table_id int64,
           read_table_id int64,
           arrival_timestamp timestamp,
           last_write_table_insert timestamp,
           next_write_table_insert timestamp,
           time_since_last_ingest_ms int64,
           time_to_next_ingest_ms int64
           )
    """
    # Create output table
    con.execute(create_output_table)


