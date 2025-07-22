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
          key_2 STRING,
          key_3 STRING,
          key_4 STRING,
          key_5 STRING,
          key_6 STRING,
          key_7 STRING,
          key_8 STRING,
          key_9 STRING,
          key_10 STRING,
          key_11 STRING,
          key_12 STRING,
          key_13 STRING,
          key_14 STRING,
          key_15 STRING,
          key_16 STRING,
          key_17 STRING,
          key_18 STRING,
          key_19 STRING,
          key_20 STRING,
          key_21 STRING,
          key_22 STRING,
          key_23 STRING,
          key_24 STRING,
          key_25 STRING,
          key_26 STRING,
          key_27 STRING,
          key_28 STRING,
          key_29 STRING,
          key_30 STRING,
          key_31 STRING,
          key_32 STRING,
          key_33 STRING,
          key_34 STRING,
          key_35 STRING,
          key_36 STRING,
          key_37 STRING,
          key_38 STRING,
          key_39 STRING,
          key_40 STRING,
          key_41 STRING,
          key_42 STRING,
          key_43 STRING,
          key_44 STRING,
          key_45 STRING,
          key_46 STRING,
          key_47 STRING,
          key_48 STRING,
          key_49 STRING,
          key_50 STRING,
          key_51 STRING,
          key_52 STRING,
          key_53 STRING,
          key_54 STRING,
          key_55 STRING,
          key_56 STRING,
          key_57 STRING,
          key_58 STRING,
          key_59 STRING,
          key_60 STRING,
          key_61 STRING,
          key_62 STRING,
          key_63 STRING,
          key_64 STRING,
          key_65 STRING,
          key_66 STRING,
          key_67 STRING,
          key_68 STRING,
          key_69 STRING,
          key_70 STRING,
          key_71 STRING,
          key_72 STRING,
          key_73 STRING,
          key_74 STRING,
          key_75 STRING,
          key_76 STRING,
          key_77 STRING,
          key_78 STRING,
          key_79 STRING,
          key_80 STRING,
          key_81 STRING,
          key_82 STRING,
          key_83 STRING,
          key_84 STRING,
          key_85 STRING,
          key_86 STRING,
          key_87 STRING,
          key_88 STRING,
          key_89 STRING,
          key_90 STRING,
          key_91 STRING,
          key_92 STRING,
          key_93 STRING,
          key_94 STRING,
          key_95 STRING,
          key_96 STRING,
          key_97 STRING,
          key_98 STRING,
          key_99 STRING,
          key_100 STRING,
          key_101 STRING,
          key_102 STRING,
          key_103 STRING,
          key_104 STRING,
          key_105 STRING,
          key_106 STRING,
          key_107 STRING,
          key_108 STRING,
          key_109 STRING,
          key_110 STRING,
          key_111 STRING,
          key_112 STRING,
          key_113 STRING,
          key_114 STRING,
          key_115 STRING,
          key_116 STRING,
          key_117 STRING,
          key_118 STRING,
          key_119 STRING,
          key_120 STRING,
          key_121 STRING,
          key_122 STRING,
          key_123 STRING,
          key_124 STRING,
          key_125 STRING
        ) WITH (
          'connector' = 'filesystem',
          'path' = 'hdfs://namenode:8020/user/flink/output/',
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
