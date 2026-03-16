from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"

KAFKA_BOOTSTRAP = "redpanda:29092"
KAFKA_TOPIC = "green-trips"

SINK_TABLE = "green_trips_session_counts"


def create_source_table(t_env: StreamTableEnvironment) -> str:
    source_table = "green_trips_session_source"

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
    sink_table = "green_trips_session_sink"

    sink_ddl = f"""
    CREATE TABLE {sink_table} (
        session_start TIMESTAMP(3),
        session_end TIMESTAMP(3),
        PULocationID INT,
        num_trips BIGINT,
        PRIMARY KEY (session_start, session_end, PULocationID) NOT ENFORCED
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
    query_table = "green_trips_session_query_source"

    source_ddl = f"""
    CREATE TABLE {query_table} (
        session_start TIMESTAMP(3),
        session_end TIMESTAMP(3),
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


def run_session_window_job() -> None:
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
        window_start AS session_start,
        window_end AS session_end,
        PULocationID,
        COUNT(*) AS num_trips
    FROM TABLE(
        SESSION(TABLE {source_table}, DESCRIPTOR(event_ts), INTERVAL '5' MINUTES)
    )
    GROUP BY window_start, window_end, PULocationID
    """

    t_env.execute_sql(insert_sql).wait()

    query_table = create_query_source_table(t_env)
    result = t_env.execute_sql(f"""
        SELECT PULocationID, num_trips, session_start, session_end
        FROM {query_table}
        ORDER BY num_trips DESC
        LIMIT 1
    """)

    with result.collect() as results:
        for row in results:
            pu_location_id = row[0]
            num_trips = row[1]
            session_start = row[2]
            session_end = row[3]
            print("Longest session by trip count:")
            print(
                f"PULocationID={pu_location_id}, num_trips={num_trips}, "
                f"session_start={session_start}, session_end={session_end}"
            )


if __name__ == "__main__":
    run_session_window_job()
