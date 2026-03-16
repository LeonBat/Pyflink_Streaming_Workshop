from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"

KAFKA_BOOTSTRAP = "redpanda:29092"
KAFKA_TOPIC = "green-trips"

SINK_TABLE = "green_trips_5min_counts"


def create_source_table(t_env: StreamTableEnvironment) -> str:
    source_table = "green_trips_source"

    source_ddl = f"""
    CREATE TABLE {source_table} (
        lpep_pickup_datetime STRING,
        PULocationID INT,
        event_ts AS TO_TIMESTAMP(REPLACE(lpep_pickup_datetime, 'T', ' ')),
        WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
        'topic' = '{KAFKA_TOPIC}',
        'scan.startup.mode' = 'earliest-offset',
        'scan.bounded.mode' = 'latest-offset',
        'properties.auto.offset.reset' = 'earliest',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true'
    )
    """

    t_env.execute_sql(source_ddl)
    return source_table


def create_sink_table(t_env: StreamTableEnvironment) -> str:
    sink_table = "green_trips_sink"

    sink_ddl = f"""
    CREATE TABLE {sink_table} (
        window_start TIMESTAMP(3),
        PULocationID INT,
        num_trips BIGINT,
        PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}',
        'table-name' = '{SINK_TABLE}',
        'username' = '{POSTGRES_USER}',
        'password' = '{POSTGRES_PASSWORD}',
        'driver' = 'org.postgresql.Driver'
    )
    """

    t_env.execute_sql(sink_ddl)
    return sink_table


def create_query_source_table(t_env: StreamTableEnvironment) -> str:
    query_table = "green_trips_counts_query_source"

    source_ddl = f"""
    CREATE TABLE {query_table} (
        window_start TIMESTAMP(3),
        PULocationID INT,
        num_trips BIGINT
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}',
        'table-name' = '{SINK_TABLE}',
        'username' = '{POSTGRES_USER}',
        'password' = '{POSTGRES_PASSWORD}',
        'driver' = 'org.postgresql.Driver'
    )
    """

    t_env.execute_sql(source_ddl)
    return query_table


def run_window_job() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table = create_source_table(t_env)
    sink_table = create_sink_table(t_env)

    insert_sql = f"""
    INSERT INTO {sink_table}
    SELECT
        window_start,
        PULocationID,
        COUNT(*) AS num_trips
    FROM TABLE(
        TUMBLE(TABLE {source_table}, DESCRIPTOR(event_ts), INTERVAL '5' MINUTES)
    )
    GROUP BY window_start, PULocationID
    """

    # The query terminates because the Kafka source is configured as bounded.
    t_env.execute_sql(insert_sql).wait()

    query_table = create_query_source_table(t_env)
    result = t_env.execute_sql(f"""
        SELECT PULocationID, num_trips
        FROM {query_table}
        ORDER BY num_trips DESC
        LIMIT 3
    """)

    print("Top 3 PULocationID by trips in a single 5-minute window:")
    top_3 = []
    with result.collect() as results:
        for row in results:
            pu_location_id = row[0]
            num_trips = row[1]
            top_3.append((pu_location_id, num_trips))
            print(f"PULocationID={pu_location_id}, num_trips={num_trips}")

    if top_3:
        print(f"Most trips in one 5-minute window: PULocationID {top_3[0][0]}")


def main() -> None:
    run_window_job()


if __name__ == "__main__":
    main()
