import json
import logging

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from src.config.config_manager import KafkaConfig, read_ccloud_config, read_sr_config

logger = logging.getLogger(__name__)

# Percentiles are doubles, not ints. As ints they lost sub-millisecond
# resolution, and an int32 overflowed outright once a unit mismatch inflated a
# latency past ~24 days -- which is how the old schema turned a measurement bug
# into a serialization failure.
RESULT_SCHEMA = json.dumps({
    "namespace": "kafka.latency.profiler",
    "type": "record",
    "name": "LatencyResult",
    "fields": [
        {"name": "date_time", "type": "string"},
        {"name": "topic", "type": "string"},
        {"name": "group_id", "type": ["null", "string"], "default": None},
        {"name": "partition", "type": "string"},
        {"name": "count", "type": "long"},
        {"name": "skipped", "type": "long"},
        {"name": "negative_latencies", "type": "long"},
        {"name": "window_seconds", "type": ["null", "double"], "default": None},
        {"name": "min_ms", "type": "double"},
        {"name": "mean_ms", "type": "double"},
        {"name": "max_ms", "type": "double"},
        {"name": "stddev_ms", "type": "double"},
        {"name": "p50_ms", "type": "double"},
        {"name": "p90_ms", "type": "double"},
        {"name": "p95_ms", "type": "double"},
        {"name": "p99_ms", "type": "double"},
        {"name": "p999_ms", "type": "double"},
    ],
})


def output_to_kafka(config: KafkaConfig, rows: list) -> None:
    """Output results to Kafka topic, one message per partition plus an 'all' row."""
    try:
        schema_registry_client = SchemaRegistryClient(read_sr_config(config.producer_config_file))
        avro_serializer = AvroSerializer(schema_registry_client, RESULT_SCHEMA)
        string_serializer = StringSerializer('utf_8')

        producer = Producer(read_ccloud_config(config.producer_config_file))
        for row in rows:
            key = (
                f"topic:{row['topic']},group:{row['group_id']},"
                f"partition:{row['partition']},time:{row['date_time']}"
            )
            producer.produce(
                topic=config.output_topic,
                key=string_serializer(key),
                value=avro_serializer(
                    row, SerializationContext(config.output_topic, MessageField.VALUE)
                ),
                on_delivery=delivery_report,
            )
        producer.flush()
        logger.info(f"Successfully produced {len(rows)} result(s) to topic {config.output_topic}")
    except Exception as e:
        logger.error(f"Error producing to Kafka: {str(e)}")
        raise


def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        logger.error(f"Delivery failed for message {msg.key()}: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} Partition[{msg.partition()}] at offset {msg.offset()}")
