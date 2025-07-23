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
    # t_env.execute_sql(f"""
    #     CREATE TABLE kafka_source (
    #         `key` STRING,
    #         `value` STRING,
    #         `ts` TIMESTAMP(3)
    #     ) WITH (
    #         'connector' = 'kafka',
    #         'topic' = '{topic}',
    #         'properties.bootstrap.servers' = '{kafka_bootstrap}',
    #         'properties.group.id' = '{group_id}',
    #         'scan.startup.mode' = 'earliest-offset',
    #         'format' = 'json',

    #         -- 🔐 Kerberos + SSL configuration
    #         'properties.security.protocol' = 'SASL_SSL',
    #         'properties.sasl.mechanism' = 'GSSAPI',
    #         'properties.sasl.kerberos.service.name' = '{kafka_service_name}',
    #         'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab="{keytab_path}" principal="{principal}";'
    #     )
    # """)
    t_env.execute_sql("""
        CREATE TABLE kafka_source (
          `type` STRING,
          operation STRING,
          source_table STRING,
          payload_sys_id STRING,
          `timestamp` BIGINT,
          data MAP<STRING, STRING>
        ) WITH (
          'connector' = 'kafka',
          'topic' = 'your_topic',
          'properties.bootstrap.servers' = 'your.kafka.broker:9092',
          'properties.group.id' = 'flink-flatten-group',
          'scan.startup.mode' = 'earliest-offset',
          'format' = 'json',
          'json.fail-on-missing-field' = 'false',
          'json.ignore-parse-errors' = 'true'
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
          `type` STRING,
          operation STRING,
          source_table STRING,
          payload_sys_id STRING,
          `timestamp` BIGINT,
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
          'path' = 'hdfs://namenode:8020/user/flink/flattened_output/',
          'format' = 'json'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE print_sink (
          raw_type STRING,
          raw_op STRING,
          raw_table STRING,
          raw_payload STRING,
          raw_ts BIGINT
        ) WITH (
          'connector' = 'print'
        )
    """)
    
    t_env.execute_sql("""
        INSERT INTO print_sink
        SELECT 
          `type`, 
          operation, 
          source_table, 
          payload_sys_id,
          `timestamp`
        FROM kafka_source
    """)




    # Execute insert
    # t_env.execute_sql("""
    #     INSERT INTO hdfs_sink
    #     SELECT * FROM kafka_source
    # """)

    # t_env.execute_sql("""
    #     INSERT INTO hdfs_sink
    #     SELECT
    #       JSON_VALUE(raw, '$.type') AS type,
    #       JSON_VALUE(raw, '$.operation') AS operation,
    #       JSON_VALUE(raw, '$.source_table') AS source_table,
    #       JSON_VALUE(raw, '$.payload_sys_id') AS payload_sys_id,
    #       CAST(JSON_VALUE(raw, '$.timestamp') AS BIGINT) AS timestamp,
    #       JSON_VALUE(raw, '$.data.asset_tag') AS asset_tag,
    #       JSON_VALUE(raw, '$.data.attestation_status') AS attestation_status,
    #       JSON_VALUE(raw, '$.data.business_criticality') AS business_criticality,
    #       JSON_VALUE(raw, '$.data.key_1') AS key_1,
    #       JSON_VALUE(raw, '$.data.key_2') AS key_2,
    #       JSON_VALUE(raw, '$.data.key_3') AS key_3,
    #       JSON_VALUE(raw, '$.data.key_4') AS key_4,
    #       JSON_VALUE(raw, '$.data.key_5') AS key_5,
    #       JSON_VALUE(raw, '$.data.key_6') AS key_6,
    #       JSON_VALUE(raw, '$.data.key_7') AS key_7,
    #       JSON_VALUE(raw, '$.data.key_8') AS key_8,
    #       JSON_VALUE(raw, '$.data.key_9') AS key_9,
    #       JSON_VALUE(raw, '$.data.key_10') AS key_10,
    #       JSON_VALUE(raw, '$.data.key_11') AS key_11,
    #       JSON_VALUE(raw, '$.data.key_12') AS key_12,
    #       JSON_VALUE(raw, '$.data.key_13') AS key_13,
    #       JSON_VALUE(raw, '$.data.key_14') AS key_14,
    #       JSON_VALUE(raw, '$.data.key_15') AS key_15,
    #       JSON_VALUE(raw, '$.data.key_16') AS key_16,
    #       JSON_VALUE(raw, '$.data.key_17') AS key_17,
    #       JSON_VALUE(raw, '$.data.key_18') AS key_18,
    #       JSON_VALUE(raw, '$.data.key_19') AS key_19,
    #       JSON_VALUE(raw, '$.data.key_20') AS key_20,
    #       JSON_VALUE(raw, '$.data.key_21') AS key_21,
    #       JSON_VALUE(raw, '$.data.key_22') AS key_22,
    #       JSON_VALUE(raw, '$.data.key_23') AS key_23,
    #       JSON_VALUE(raw, '$.data.key_24') AS key_24,
    #       JSON_VALUE(raw, '$.data.key_25') AS key_25,
    #       JSON_VALUE(raw, '$.data.key_26') AS key_26,
    #       JSON_VALUE(raw, '$.data.key_27') AS key_27,
    #       JSON_VALUE(raw, '$.data.key_28') AS key_28,
    #       JSON_VALUE(raw, '$.data.key_29') AS key_29,
    #       JSON_VALUE(raw, '$.data.key_30') AS key_30,
    #       JSON_VALUE(raw, '$.data.key_31') AS key_31,
    #       JSON_VALUE(raw, '$.data.key_32') AS key_32,
    #       JSON_VALUE(raw, '$.data.key_33') AS key_33,
    #       JSON_VALUE(raw, '$.data.key_34') AS key_34,
    #       JSON_VALUE(raw, '$.data.key_35') AS key_35,
    #       JSON_VALUE(raw, '$.data.key_36') AS key_36,
    #       JSON_VALUE(raw, '$.data.key_37') AS key_37,
    #       JSON_VALUE(raw, '$.data.key_38') AS key_38,
    #       JSON_VALUE(raw, '$.data.key_39') AS key_39,
    #       JSON_VALUE(raw, '$.data.key_40') AS key_40,
    #       JSON_VALUE(raw, '$.data.key_41') AS key_41,
    #       JSON_VALUE(raw, '$.data.key_42') AS key_42,
    #       JSON_VALUE(raw, '$.data.key_43') AS key_43,
    #       JSON_VALUE(raw, '$.data.key_44') AS key_44,
    #       JSON_VALUE(raw, '$.data.key_45') AS key_45,
    #       JSON_VALUE(raw, '$.data.key_46') AS key_46,
    #       JSON_VALUE(raw, '$.data.key_47') AS key_47,
    #       JSON_VALUE(raw, '$.data.key_48') AS key_48,
    #       JSON_VALUE(raw, '$.data.key_49') AS key_49,
    #       JSON_VALUE(raw, '$.data.key_50') AS key_50,
    #       JSON_VALUE(raw, '$.data.key_51') AS key_51,
    #       JSON_VALUE(raw, '$.data.key_52') AS key_52,
    #       JSON_VALUE(raw, '$.data.key_53') AS key_53,
    #       JSON_VALUE(raw, '$.data.key_54') AS key_54,
    #       JSON_VALUE(raw, '$.data.key_55') AS key_55,
    #       JSON_VALUE(raw, '$.data.key_56') AS key_56,
    #       JSON_VALUE(raw, '$.data.key_57') AS key_57,
    #       JSON_VALUE(raw, '$.data.key_58') AS key_58,
    #       JSON_VALUE(raw, '$.data.key_59') AS key_59,
    #       JSON_VALUE(raw, '$.data.key_60') AS key_60,
    #       JSON_VALUE(raw, '$.data.key_61') AS key_61,
    #       JSON_VALUE(raw, '$.data.key_62') AS key_62,
    #       JSON_VALUE(raw, '$.data.key_63') AS key_63,
    #       JSON_VALUE(raw, '$.data.key_64') AS key_64,
    #       JSON_VALUE(raw, '$.data.key_65') AS key_65,
    #       JSON_VALUE(raw, '$.data.key_66') AS key_66,
    #       JSON_VALUE(raw, '$.data.key_67') AS key_67,
    #       JSON_VALUE(raw, '$.data.key_68') AS key_68,
    #       JSON_VALUE(raw, '$.data.key_69') AS key_69,
    #       JSON_VALUE(raw, '$.data.key_70') AS key_70,
    #       JSON_VALUE(raw, '$.data.key_71') AS key_71,
    #       JSON_VALUE(raw, '$.data.key_72') AS key_72,
    #       JSON_VALUE(raw, '$.data.key_73') AS key_73,
    #       JSON_VALUE(raw, '$.data.key_74') AS key_74,
    #       JSON_VALUE(raw, '$.data.key_75') AS key_75,
    #       JSON_VALUE(raw, '$.data.key_76') AS key_76,
    #       JSON_VALUE(raw, '$.data.key_77') AS key_77,
    #       JSON_VALUE(raw, '$.data.key_78') AS key_78,
    #       JSON_VALUE(raw, '$.data.key_79') AS key_79,
    #       JSON_VALUE(raw, '$.data.key_80') AS key_80,
    #       JSON_VALUE(raw, '$.data.key_81') AS key_81,
    #       JSON_VALUE(raw, '$.data.key_82') AS key_82,
    #       JSON_VALUE(raw, '$.data.key_83') AS key_83,
    #       JSON_VALUE(raw, '$.data.key_84') AS key_84,
    #       JSON_VALUE(raw, '$.data.key_85') AS key_85,
    #       JSON_VALUE(raw, '$.data.key_86') AS key_86,
    #       JSON_VALUE(raw, '$.data.key_87') AS key_87,
    #       JSON_VALUE(raw, '$.data.key_88') AS key_88,
    #       JSON_VALUE(raw, '$.data.key_89') AS key_89,
    #       JSON_VALUE(raw, '$.data.key_90') AS key_90,
    #       JSON_VALUE(raw, '$.data.key_91') AS key_91,
    #       JSON_VALUE(raw, '$.data.key_92') AS key_92,
    #       JSON_VALUE(raw, '$.data.key_93') AS key_93,
    #       JSON_VALUE(raw, '$.data.key_94') AS key_94,
    #       JSON_VALUE(raw, '$.data.key_95') AS key_95,
    #       JSON_VALUE(raw, '$.data.key_96') AS key_96,
    #       JSON_VALUE(raw, '$.data.key_97') AS key_97,
    #       JSON_VALUE(raw, '$.data.key_98') AS key_98,
    #       JSON_VALUE(raw, '$.data.key_99') AS key_99,
    #       JSON_VALUE(raw, '$.data.key_100') AS key_100,
    #       JSON_VALUE(raw, '$.data.key_101') AS key_101,
    #       JSON_VALUE(raw, '$.data.key_102') AS key_102,
    #       JSON_VALUE(raw, '$.data.key_103') AS key_103,
    #       JSON_VALUE(raw, '$.data.key_104') AS key_104,
    #       JSON_VALUE(raw, '$.data.key_105') AS key_105,
    #       JSON_VALUE(raw, '$.data.key_106') AS key_106,
    #       JSON_VALUE(raw, '$.data.key_107') AS key_107,
    #       JSON_VALUE(raw, '$.data.key_108') AS key_108,
    #       JSON_VALUE(raw, '$.data.key_109') AS key_109,
    #       JSON_VALUE(raw, '$.data.key_110') AS key_110,
    #       JSON_VALUE(raw, '$.data.key_111') AS key_111,
    #       JSON_VALUE(raw, '$.data.key_112') AS key_112,
    #       JSON_VALUE(raw, '$.data.key_113') AS key_113,
    #       JSON_VALUE(raw, '$.data.key_114') AS key_114,
    #       JSON_VALUE(raw, '$.data.key_115') AS key_115,
    #       JSON_VALUE(raw, '$.data.key_116') AS key_116,
    #       JSON_VALUE(raw, '$.data.key_117') AS key_117,
    #       JSON_VALUE(raw, '$.data.key_118') AS key_118,
    #       JSON_VALUE(raw, '$.data.key_119') AS key_119,
    #       JSON_VALUE(raw, '$.data.key_120') AS key_120,
    #       JSON_VALUE(raw, '$.data.key_121') AS key_121,
    #       JSON_VALUE(raw, '$.data.key_122') AS key_122,
    #       JSON_VALUE(raw, '$.data.key_123') AS key_123,
    #       JSON_VALUE(raw, '$.data.key_124') AS key_124,
    #       JSON_VALUE(raw, '$.data.key_125') AS key_125
    #     FROM kafka_source
    # """)
    t_env.execute_sql(f"""
        INSERT INTO hdfs_sink
        SELECT
          `type`,
          operation,
          source_table,
          payload_sys_id,
          `timestamp`,
          {', '.join([f"data['key_{i}']" for i in range(1, 126)])}
        FROM kafka_source
    """)


if __name__ == "__main__":
    main()
