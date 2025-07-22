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
    # t_env.execute_sql(f"""
    #     CREATE TABLE hdfs_sink (
    #         `key` STRING,
    #         `value` STRING,
    #         `ts` TIMESTAMP(3)
    #     ) WITH (
    #         'connector' = 'filesystem',
    #         'path' = '{output_path}',
    #         'format' = 'json'
    #     )
    # """)

    t_env.execute_sql("""
        CREATE TABLE hdfs_sink (
          type STRING,
          operation STRING,
          source_table STRING,
          payload_sys_id STRING,
          timestamp BIGINT,
          asset_tag STRING,
          attestation_status STRING,
          business_criticality STRING,
          key_1 STRING,
          key_125 STRING
        ) WITH (
          'connector' = 'filesystem',
          'path' = 'hdfs://namenode:8020/user/flink/output',
          'format' = 'json'
        )
    """)


    # Execute insert
    # t_env.execute_sql("""
    #     INSERT INTO hdfs_sink
    #     SELECT * FROM kafka_source
    # """)

    t_env.execute_sql("""
        INSERT INTO hdfs_sink
        SELECT
          JSON_VALUE(raw, '$.type') AS type,
          JSON_VALUE(raw, '$.operation') AS operation,
          JSON_VALUE(raw, '$.source_table') AS source_table,
          JSON_VALUE(raw, '$.payload_sys_id') AS payload_sys_id,
          CAST(JSON_VALUE(raw, '$.timestamp') AS BIGINT) AS timestamp,
          JSON_VALUE(raw, '$.data.asset_tag') AS asset_tag,
          JSON_VALUE(raw, '$.data.attestation_status') AS attestation_status,
          JSON_VALUE(raw, '$.data.business_criticality') AS business_criticality,
          JSON_VALUE(raw, '$.data.key_1') AS key_1,
          JSON_VALUE(raw, '$.data.key_125') AS key_125
        FROM kafka_source
    """)

if __name__ == "__main__":
    main()
