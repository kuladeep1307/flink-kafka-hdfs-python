from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    kafka_bootstrap = "kafka-broker1:9093,kafka-broker2:9093"
    topic = "secure-topic"
    group_id = "flink-secure-group"
    output_path = "hdfs://namenode:8020/user/flink/output"

    principal = "flinkuser@YOUR.REALM.COM"
    keytab_path = "/etc/security/keytabs/flink.keytab"
    kafka_service_name = "kafka"

    # Define Kafka source with Kerberos + SSL
    t_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            `key` STRING,
            `value` STRING,
            `ts` TIMESTAMP(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{kafka_bootstrap}',
            'properties.group.id' = '{group_id}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',

            -- 🔐 Kerberos + SSL configuration
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'GSSAPI',
            'properties.sasl.kerberos.service.name' = '{kafka_service_name}',
            'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab="{keytab_path}" principal="{principal}";'
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

    # Execute insert
    t_env.execute_sql("""
        INSERT INTO hdfs_sink
        SELECT * FROM kafka_source
    """)

if __name__ == "__main__":
    main()
