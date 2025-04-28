import os
from typing import Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class KafkaConfig:
    consumer_config_file: str
    producer_config_file: Optional[str]
    input_topic: str
    group_id: str
    enable_sampling: bool
    run_interval: int
    t1: str
    t2: str
    output_type: str
    local_filepath: Optional[str]
    output_topic: Optional[str]
    value_deserializer: str
    key_deserializer: str
    date_time_format: str

def validate_config(config: KafkaConfig) -> None:
    """Validate the configuration parameters."""
    if config.value_deserializer not in ['AvroDeserializer', 'JSONDeserializer', 'StringDeserializer', 'JSONSchemaDeserializer']:
        raise ValueError('Invalid input for VALUE_DESERIALIZER must be among AvroDeserializer,JSONDeserializer,StringDeserializer,JSONSchemaDeserializer')
    
    if config.key_deserializer not in ['AvroDeserializer', 'JSONDeserializer', 'StringDeserializer', 'JSONSchemaDeserializer']:
        raise ValueError('Invalid input for KEY_DESERIALIZER must be among AvroDeserializer,JSONDeserializer,StringDeserializer,JSONSchemaDeserializer')
    
    if config.t1 != "IngestionTime" and not (config.t1.startswith('key.') or config.t1.startswith('value.')):
        raise ValueError('Invalid input for T1 must be among IngestionTime or value.column name or key.column name')
    
    if config.t2 not in ['IngestionTime', 'consumerWallClockTime']:
        raise ValueError("Invalid input for T2 must be one of IngestionTime, consumerWallClockTime")
    
    if config.t1 == config.t2 == 'IngestionTime':
        raise ValueError("Both T1 and T2 cannot be IngestionTime")
    
    if config.output_type == 'dumpToTopic':
        if not config.producer_config_file:
            raise ValueError("To store latency measured to kafka topic must provide producer configuration file path to PRODUCER_CONFIG_FILE")
        if not config.output_topic:
            raise ValueError("To store latency measured to kafka topic, topic name must be provided in OUTPUT_TOPIC")
    
    if config.output_type == 'localFileDump' and not config.local_filepath:
        raise ValueError("To store latency measured to local file, file path must be provided in RESULT_DUMP_LOCAL_FILEPATH")

def create_kafka_config() -> KafkaConfig:
    """Create and validate Kafka configuration from environment variables."""
    config = KafkaConfig(
        consumer_config_file=os.getenv("CONSUMER_CONFIG_FILE"),
        producer_config_file=os.getenv("PRODUCER_CONFIG_FILE"),
        input_topic=os.getenv("INPUT_TOPIC"),
        group_id=os.getenv("GROUP_ID"),
        enable_sampling=os.getenv("ENABLE_SAMPLING") == 'True',
        run_interval=int(os.getenv("RUN_INTERVAL", "0")),
        t1=os.getenv("T1"),
        t2=os.getenv("T2"),
        output_type=os.getenv("CONSUMER_OUTPUT"),
        local_filepath=os.getenv("RESULT_DUMP_LOCAL_FILEPATH"),
        output_topic=os.getenv("OUTPUT_TOPIC"),
        value_deserializer=os.getenv("VALUE_DESERIALIZER"),
        key_deserializer=os.getenv("KEY_DESERIALIZER"),
        date_time_format=os.getenv("DATE_TIME_FORMAT")
    )
    validate_config(config)
    return config

def read_ccloud_config(config_file: str) -> dict:
    """Read and parse Kafka configuration file."""
    conf = {}
    try:
        with open(config_file) as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    conf[parameter] = value.strip()
        conf.pop('schema.registry.url', None)
        conf.pop('basic.auth.user.info', None)
        conf.pop('basic.auth.credentials.source', None)
        return conf
    except Exception as e:
        logger.error(f"Error reading config file {config_file}: {str(e)}")
        raise

def read_sr_config(config_file: str) -> dict:
    """Read and parse Schema Registry configuration."""
    conf = {}
    try:
        with open(config_file) as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    conf[parameter] = value.strip()
        return {
            'url': conf['schema.registry.url'],
            'basic.auth.user.info': conf['basic.auth.user.info']
        }
    except Exception as e:
        logger.error(f"Error reading schema registry config file {config_file}: {str(e)}")
        raise 