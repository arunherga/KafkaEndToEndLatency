import logging
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer

from src.config.config_manager import KafkaConfig, read_ccloud_config, read_sr_config

logger = logging.getLogger(__name__)

def output_to_kafka(config: KafkaConfig, avg: float, quantiles: list, date_string: str) -> None:
    """Output results to Kafka topic."""
    schema_string = """
    {
        "namespace": "example.avro",
        "type": "record",
        "name": "result",
        "fields": [
            {"name": "average", "type": "int"},
            {"name": "percentile50", "type": "int"},
            {"name": "percentile90", "type": "int"},
            {"name": "percentile95", "type": "int"},
            {"name": "percentile99", "type": "int"},
            {"name": "percentile999", "type": "int"},
            {"name": "Date_Time", "type": "string"}
        ]
    }
    """
    
    result = {
        "average": int(avg),
        "percentile50": int(quantiles[0]),
        "percentile90": int(quantiles[1]),
        "percentile95": int(quantiles[2]),
        "percentile99": int(quantiles[3]),
        "percentile999": int(quantiles[4]),
        "Date_Time": date_string
    }
    
    try:
        schema_registry_client = SchemaRegistryClient(read_sr_config(config.producer_config_file))
        avro_serializer = AvroSerializer(schema_registry_client, schema_string)
        string_serializer = StringSerializer('utf_8')
        
        producer = Producer(read_ccloud_config(config.producer_config_file))
        producer.produce(
            topic=config.output_topic,
            key=string_serializer(f"Topic:{config.input_topic},consumer group id:{config.group_id},Date Time:{date_string}"),
            value=avro_serializer(result, SerializationContext(config.output_topic, MessageField.VALUE)),
            on_delivery=delivery_report
        )
        producer.flush()
        logger.info(f"Successfully produced results to topic {config.output_topic}")
    except Exception as e:
        logger.error(f"Error producing to Kafka: {str(e)}")
        raise

def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        logger.error(f"Delivery failed for message {msg.key()}: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} Partition[{msg.partition()}] at offset {msg.offset()}") 