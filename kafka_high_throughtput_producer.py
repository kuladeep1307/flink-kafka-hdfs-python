import json
import random
import string
import time
from confluent_kafka import Producer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka-producer")

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'servicenow_updates'
NULL_CHANCE = 0.3  # 30%

OPERATIONS = ["insert", "update", "delete"]
SOURCE_TABLES = ["cmdb_ci_service_auto", "cmdb_ci_server", "cmdb_ci_database", "cmdb_ci_web_service"]
ATTESTATION_STATUS = ["Not yet reviewed", "Approved", "Rejected", "Pending"]
BUSINESS_CRITICALITY = [
    "1 - most critical",
    "2 - highly critical",
    "3 - moderately critical",
    "4 - not critical"
]

def random_string(length=16):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def maybe_null(value_func):
    if random.random() < NULL_CHANCE:
        return None
    else:
        return value_func()

def generate_data():
    data = {
        "aliases": None,
        "asset": maybe_null(lambda: random_string()),
        "asset_tag": maybe_null(lambda: random_string()),
        "assigned_to": maybe_null(lambda: random_string()),
        "attestation_status": maybe_null(lambda: random.choice(ATTESTATION_STATUS)),
        "business_criticality": maybe_null(lambda: random.choice(BUSINESS_CRITICALITY))
    }
    # Add 125 random keys
    for i in range(1, 126):
        key = f"key_{i}"
        data[key] = maybe_null(lambda: random_string())
    return data

def generate_message():
    return {
        "type": "record",
        "operation": random.choice(OPERATIONS),
        "source_table": random.choice(SOURCE_TABLES),
        "payload_sys_id": random_string(),
        "timestamp": int(time.time() * 1000),
        "data": generate_data()
    }

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def main():
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'compression.type': 'snappy',      # Compress for better throughput
        'linger.ms': 20,                   # Buffer time for batching (20ms)
        'batch.num.messages': 1000,        # Batch size
        'queue.buffering.max.messages': 100000,
        'enable.idempotence': True,       # Exactly once producer semantics
        'acks': 'all',                    # Wait for full commit
    }

    producer = Producer(conf)

    logger.info("Starting high throughput Kafka producer...")
    message_count = 0

    try:
        while True:
            msg = generate_message()
            producer.produce(
                TOPIC,
                key=msg["payload_sys_id"],
                value=json.dumps(msg),
                callback=delivery_report
            )
            message_count += 1

            # Poll to trigger delivery report callbacks
            producer.poll(0)

            # Optional: Control message rate (remove to max throughput)
            # time.sleep(0.01)  # ~100 msg/sec

            if message_count % 1000 == 0:
                logger.info(f"Produced {message_count} messages so far")

    except KeyboardInterrupt:
        logger.info("Interrupted by user")

    finally:
        logger.info("Flushing pending messages...")
        producer.flush()
        logger.info("Producer closed.")

if __name__ == "__main__":
    main()
