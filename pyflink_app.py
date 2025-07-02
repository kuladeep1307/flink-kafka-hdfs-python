import argparse
import os

from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FileSink, OutputFileConfig
from pyflink.datastream.connectors.file_system import StreamFormat
from pyflink.common.watermark_strategy import WatermarkStrategy


def parse_args():
    parser = argparse.ArgumentParser(description="Flink Kafka to HDFS with Kerberos")

    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--group-id", default="flink-consumer-group")

    parser.add_argument("--principal", required=True)
    parser.add_argument("--keytab-path", required=True)
    parser.add_argument("--krb5-conf", required=True)

    parser.add_argument("--kafka-service-name", default="kafka")
    parser.add_argument("--output-path", required=True)

    return parser.parse_args()


def main():
    args = parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # Set system properties for Kerberos
    os.environ["java.security.krb5.conf"] = args.krb5_conf
    os.environ["java.security.auth.login.config"] = "/tmp/jaas.conf"  # Optional: generated below

    # Generate inline JAAS config and write to temp file (useful if not globally configured)
    jaas_content = f"""
KafkaClient {{
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  storeKey=true
  keyTab="{args.keytab_path}"
  principal="{args.principal}"
  useTicketCache=false
  debug=true;
}};
"""
    with open("/tmp/jaas.conf", "w") as f:
        f.write(jaas_content)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(args.bootstrap_servers) \
        .set_topics(args.topic) \
        .set_group_id(args.group_id) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaSource.OffsetInitializer.earliest()) \
        .set_property("security.protocol", "SASL_PLAINTEXT") \
        .set_property("sasl.mechanism", "GSSAPI") \
        .set_property("sasl.kerberos.service.name", args.kafka_service_name) \
        .build()

    stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        type_info=Types.STRING()
    )

    sink = FileSink \
        .for_row_format(args.output_path, StreamFormat.text_line_format()) \
        .with_output_file_config(
            OutputFileConfig.builder()
            .with_part_prefix("part")
            .with_part_suffix(".txt")
            .build()
        ).build()

    stream.sink_to(sink)

    env.execute("Kafka to HDFS with Kerberos")


if __name__ == "__main__":
    main()
