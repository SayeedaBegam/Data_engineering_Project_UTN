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
#################### INSERT AND UPDATE QUERIES ##################################
# timestamps over which time period the queries will run
start = '2024-03-01 00:00:00'
end = '2024-03-01 12:00:00'

insert_into_ingestion_intervals_per_table = f"""
    INSERT INTO ingestion_intervals_per_table (
    instance_id, 
    query_id, 
    write_table_id, 
    current_timestamp, 
    next_timestamp
    )   
    -- this query performs a self join to find the time difference between two subsequent ingestion queries.
    -- queries for which no next arrival timestep exists (yet), will be shown as NULL in the next_timestamp_arrival column
    SELECT
        t1.instance_id,
        t1.query_id,
        t1.write_table_id,
        t1.arrival_timestamp AS current_timestamp,
        t2.arrival_timestamp AS next_timestamp
    FROM query_count_per_table t1
    LEFT JOIN query_count_per_table t2 -- left join to keep entries where no next arrival timestamp exists
        ON t1.write_table_id = t2.write_table_id     -- should write on the same table
        AND t1.instance_id = t2.instance_id         -- only consider queries on the same instance
        AND t2.arrival_timestamp = (
            SELECT MIN(arrival_timestamp)
            FROM query_count_per_table t0
            WHERE t0.write_table_id = t1.write_table_id
            AND t0.instance_id = t1.instance_id
            AND t0.arrival_timestamp > t1.arrival_timestamp
            AND t0.query_type IN ('insert', 'copy') -- only consider ingestion queries
            AND t0.arrival_timestamp BETWEEN '{start}' AND '{end}'
        )
    WHERE 1
        AND t1.query_type IN ('insert', 'copy') -- only consider ingestion queries
        AND t1.arrival_timestamp BETWEEN '{start}' AND '{end}'
        --AND t2.arrival_timestamp BETWEEN '{start}' AND '{end}'
"""

# ingestion_intervals_per_table are updated when their next_timestamp is NULL and a new next_timestamp is available
# no timestamp filters, runs on full datatable
update_ingestion_intervals_per_table = """
    UPDATE ingestion_intervals_per_table target
    SET next_timestamp = source.next_timestamp
    FROM (
        SELECT
            t1.instance_id,
            t1.query_id,
            t1.write_table_id,
            t1.arrival_timestamp AS current_timestamp,
            t2.arrival_timestamp AS next_timestamp
        FROM query_count_per_table t1
        LEFT JOIN query_count_per_table t2
            ON t1.write_table_id = t2.write_table_id
            AND t1.instance_id = t2.instance_id
            AND t2.arrival_timestamp = (
                SELECT MIN(arrival_timestamp)
                FROM query_count_per_table t0
                WHERE t0.write_table_id = t1.write_table_id
                AND t0.instance_id = t1.instance_id
                AND t0.arrival_timestamp > t1.arrival_timestamp
                AND t0.query_type IN ('insert', 'copy')
            )
        WHERE t1.query_type IN ('insert', 'copy')
    ) AS source
    WHERE target.query_id = source.query_id
    AND target.instance_id = source.instance_id
    AND target.next_timestamp IS NULL
    AND source.next_timestamp IS NOT NULL;
"""

# match all queries to their corresponding ingestion timestamps (last and next)
# number of select queries vs. transformation queries between ingestions
# only considers select queries that reference tables where an insert has run previously
# filters out queries if no ingest query has run previously on the referenced write table (for delete and update) or the referenced read table (for select)

# optional parameter to filter on instance_id
--instance_id = 0

insert_into_output_table = f"""
INSERT INTO output_table(
         instance_id,
           query_id,
           query_type,
           write_table_id,
           read_table_id,
           arrival_timestamp,
           last_write_table_insert,
           next_write_table_insert,
           time_since_last_ingest_ms,
           time_to_next_ingest_ms
           )
WITH output AS (
SELECT
    q.instance_id,
    q.query_id,
    q.query_type,
    q.write_table_id,
    q.read_table_id,
    q.arrival_timestamp,
    i.current_timestamp AS last_write_table_insert,
    i.next_timestamp AS next_write_table_insert
FROM query_count_per_table q
LEFT JOIN ingestion_intervals_per_table i
    ON q.write_table_id = i.write_table_id
    AND q.query_id = i.query_id
    AND q.instance_id = i.instance_id
WHERE 1
    --AND q.instance_id = {instance_id}
    AND q.arrival_timestamp BETWEEN '{start}' AND '{end}'
) 
SELECT 
    o.instance_id,
    o.query_id,
    o.query_type,
    o.write_table_id,
    o.read_table_id,
    o.arrival_timestamp,
    i.last_write_table_insert,
    i.next_write_table_insert,
    EPOCH_MS(o.arrival_timestamp - i.last_write_table_insert) AS time_since_last_ingest_ms,
    EPOCH_MS(i.next_write_table_insert - o.arrival_timestamp) AS time_to_next_ingest_ms   
FROM output o
JOIN output AS i
    ON i.query_type IN ('insert','copy') -- will make cartesian product with table i filtered by query_type
    AND o.arrival_timestamp BETWEEN 
        -- CASE
        --    WHEN o.arrival_timestamp < i.last_write_table_insert 
        --    THEN STRPTIME('01.01.1900', '%d.%m.%Y')::TIMESTAMP --, set date s.t. query will not be filtered out
        --    ELSE i.last_write_table_insert
        -- END
        i.last_write_table_insert --if no ingest query has run before, the query will be filtered out
        AND COALESCE(i.next_write_table_insert, STRPTIME('31.12.2999', '%d.%m.%Y')::TIMESTAMP) --to handle NULL values
    AND o.instance_id = i.instance_id
    AND (
        (o.query_type = 'select' AND o.read_table_id = i.write_table_id) -- a select query should match the read with the write table; if a write query hasn't run before, the corresponding select query will be filtered out
        OR (o.query_type != 'select' AND o.write_table_id = i.write_table_id)  -- all other queries should math the write tables
    )
WHERE 1
    AND o.query_type NOT IN ('insert', 'copy') -- ingestion queries would show as duplicates therefore filter and add in the union all
    --AND o.instance_id = {instance_id}
    AND o.arrival_timestamp BETWEEN '{start}' AND '{end}'
-- add back the ingestion queries
UNION ALL  
SELECT  
    instance_id,
    query_id,
    query_type,
    write_table_id,
    read_table_id,
    arrival_timestamp,
    last_write_table_insert,
    next_write_table_insert,
    EPOCH_MS(arrival_timestamp - last_write_table_insert) AS time_since_last_ingest_ms,
    EPOCH_MS(next_write_table_insert - arrival_timestamp) AS time_to_next_ingest_ms   
FROM output
WHERE 1
    AND query_type IN ('insert', 'copy')
    --AND instance_id = {instance_id}
    AND arrival_timestamp BETWEEN '{start}' AND '{end}'
ORDER BY o.arrival_timestamp   
"""

# view to count analytical queries vs. transform queries per table_id
create_view_tables_workload_count = """
CREATE OR REPLACE VIEW tables_workload_count AS 
WITH select_count_table AS (        
    SELECT --count select queries by read_table_ids
        instance_id,
        read_table_id AS table_read_by_select,
        COUNT(CASE WHEN query_type = 'select' THEN 1 END) AS select_count
    FROM output_table
    WHERE query_type = 'select'
    AND read_table_id != 999999 -- only clean data, 999999 stands for Null
    AND instance_id = 0
    GROUP BY ALL
), transform_count_table AS (
    SELECT --count transformation queries by write_table_id
        instance_id,
        write_table_id AS table_transformed,
        COUNT(CASE WHEN query_type IN ('update', 'delete') THEN 1 END) AS transform_count
    FROM output_table
    WHERE query_type IN ('update', 'delete')
    AND write_table_id != 999999 -- only clean data, 999999 stands for Null
    AND instance_id = 0
    GROUP BY ALL
)
SELECT 
   COALESCE(s.instance_id, t.instance_id) AS instance_id,
    COALESCE(t.table_transformed, s.table_read_by_select) AS table_id,
    t.transform_count,
    s.select_count,
FROM select_count_table s
FULL OUTER JOIN transform_count_table t
ON t.table_transformed = s.table_read_by_select
"""

############### DASHBOARD QUERIES ############################

# compares the average time from last ingest to when the analytical query was run to the average time to the next ingest in order to determine
# data freshness. Filters tables where the average time from last ingest exceeds average time to next ingest
average_times_ingestion_analytics = """
WITH analytical_tables AS (
SELECT  -- get the tables that are identified as tables for analytical workflow
    instance_id,
    table_id,
CAST(COALESCE(select_count / (transform_count + select_count), 0) AS DECIMAL(20, 2)) AS percentage_select_queries          
FROM tables_workload_count
WHERE percentage_select_queries > 0.80
)
SELECT 
    instance_id, 
    read_table_id,
    CAST(AVG(time_since_last_ingest_ms) / 1000.0 AS DECIMAL(20, 0)) AS average_time_since_last_ingest_s, 
    CAST(AVG(time_to_next_ingest_ms) / 1000.0 AS DECIMAL(20, 0)) AS average_time_to_next_ingest_s
FROM output_table
WHERE 1 
    AND read_table_id IN ( 
        SELECT table_id
        FROM analytical_tables) -- only consider analytial tables that were read
    AND query_type = 'select'
GROUP BY instance_id, read_table_id
HAVING average_time_since_last_ingest_s > average_time_to_next_ingest_s --potential data freshness issues
"""


