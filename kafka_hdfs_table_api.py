from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Set HDFS output format location
    output_path = "hdfs://namenode:8020/user/flink/output"

    # Define Kafka source table
    t_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            `key` STRING,
            `value` STRING,
            `ts` TIMESTAMP(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'your-topic',
            'properties.bootstrap.servers' = 'your.kafka.broker:9092',
            'properties.group.id' = 'flink-group',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)

    # Define HDFS sink table
    t_env.execute_sql(f"""
        CREATE TABLE hdfs_sink (
            `key` STRING,
            `value` STRING,
            `ts` TIMESTAMP(3)
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{output_path}',
            'format' = 'json'
        )
    """)

    # Insert data into HDFS
    t_env.execute_sql("""
        INSERT INTO hdfs_sink
        SELECT * FROM kafka_source
    """)

if __name__ == "__main__":
    main()
