from kafka import KafkaProducer
import json
import random
import string
import time

# Function to generate random data
def random_data():
    keys = ["aliases", "asset", "asset_tag", "assigned_to", "attestation_status", "business_criticality"]
    values = {}
    for k in keys:
        if random.random() < 0.2:  # 20% chance of null
            values[k] = None
        else:
            values[k] = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
    return values

producer = KafkaProducer(
    bootstrap_servers=["broker1:9092", "broker2:9092"],
    security_protocol="SASL_PLAINTEXT",   # Or SASL_SSL if TLS is enabled
    sasl_mechanism="GSSAPI",
    sasl_kerberos_service_name="kafka",   # This must match Kafka config
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "my_topic"

try:
    while True:
        msg = {
            "type": "record",
            "operation": "update",
            "source_table": "cmdb_ci_service_auto",
            "payload_sys_id": ''.join(random.choices(string.ascii_lowercase + string.digits, k=26)),
            "timestamp": int(time.time() * 1000),
            "data": random_data()
        }
        producer.send(topic, value=msg)
        time.sleep(0.01)  # ~100 msgs/sec
except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    producer.flush()
    producer.close()
