import logging
import os
from dataclasses import dataclass
from typing import Optional

from src.core.timestamps import EPOCH_UNITS, TIMEZONES, TimestampError, split_timestamp_spec

logger = logging.getLogger(__name__)

VALID_DESERIALIZERS = [
    'AvroDeserializer',
    'JSONDeserializer',
    'StringDeserializer',
    'JSONSchemaDeserializer',
    'ProtobufDeserializer',
]

VALID_OUTPUT_TYPES = ['dumpToTopic', 'localFileDump']

VALID_T2 = ['IngestionTime', 'consumerWallClockTime']

# Settings with no usable default -- a run cannot be interpreted without them.
REQUIRED_SETTINGS = {
    'consumer_config_file': 'CONSUMER_CONFIG_FILE',
    'input_topic': 'INPUT_TOPIC',
    't1': 'T1',
    't2': 'T2',
    'output_type': 'CONSUMER_OUTPUT',
    'value_deserializer': 'VALUE_DESERIALIZER',
    'key_deserializer': 'KEY_DESERIALIZER',
    'date_time_format': 'DATE_TIME_FORMAT',
}

@dataclass
class KafkaConfig:
    consumer_config_file: str
    producer_config_file: Optional[str]
    input_topic: str
    group_id: str
    run_interval: int
    t1: str
    t2: str
    output_type: str
    local_filepath: Optional[str]
    output_topic: Optional[str]
    value_deserializer: str
    key_deserializer: str
    date_time_format: str
    # Fully-qualified message to decode when a .proto declares more than one.
    # Without it the first message in the schema is used, which is what
    # Confluent's default message index refers to.
    protobuf_message_name: Optional[str] = None
    # Unit of a numeric T1 field. Only meaningful when DATE_TIME_FORMAT=epoch,
    # where the raw value carries no unit of its own.
    t1_unit: str = 'ms'
    # How to interpret a parsed timestamp that carries no zone of its own.
    t1_timezone: str = 'utc'

def validate_config(config: KafkaConfig) -> None:
    """Validate the configuration parameters."""
    for attribute, env_var in REQUIRED_SETTINGS.items():
        if not getattr(config, attribute):
            raise ValueError(f"{env_var} is required but was not set")

    if config.run_interval <= 0:
        raise ValueError(f"RUN_INTERVAL must be a positive number of seconds, got {config.run_interval}")

    if config.value_deserializer not in VALID_DESERIALIZERS:
        raise ValueError(
            f'Invalid input for VALUE_DESERIALIZER must be among '
            f'{", ".join(VALID_DESERIALIZERS)}'
        )

    if config.key_deserializer not in VALID_DESERIALIZERS:
        raise ValueError(
            f'Invalid input for KEY_DESERIALIZER must be among '
            f'{", ".join(VALID_DESERIALIZERS)}'
        )

    try:
        split_timestamp_spec(config.t1)
    except TimestampError as e:
        raise ValueError(f"Invalid input for T1: {e}") from e

    if config.t2 not in VALID_T2:
        raise ValueError("Invalid input for T2 must be one of IngestionTime, consumerWallClockTime")

    if config.t1 == config.t2 == 'IngestionTime':
        raise ValueError("Both T1 and T2 cannot be IngestionTime")

    if config.t1_unit not in EPOCH_UNITS:
        raise ValueError(f"Invalid input for T1_UNIT must be one of {', '.join(sorted(EPOCH_UNITS))}")

    if config.t1_timezone not in TIMEZONES:
        raise ValueError(f"Invalid input for T1_TIMEZONE must be one of {', '.join(TIMEZONES)}")

    if config.output_type not in VALID_OUTPUT_TYPES:
        raise ValueError(
            f"Invalid input for CONSUMER_OUTPUT must be one of {', '.join(VALID_OUTPUT_TYPES)}"
        )

    if config.output_type == 'dumpToTopic':
        if not config.producer_config_file:
            raise ValueError(
                "To store latency measured to kafka topic must provide producer "
                "configuration file path to PRODUCER_CONFIG_FILE"
            )
        if not config.output_topic:
            raise ValueError("To store latency measured to kafka topic, topic name must be provided in OUTPUT_TOPIC")

    if config.output_type == 'localFileDump' and not config.local_filepath:
        raise ValueError(
            "To store latency measured to local file, file path must be provided in "
            "RESULT_DUMP_LOCAL_FILEPATH"
        )

def create_kafka_config() -> KafkaConfig:
    """Create and validate Kafka configuration from environment variables."""
    if os.getenv("ENABLE_SAMPLING") is not None:
        logger.warning(
            "ENABLE_SAMPLING is no longer supported and is being ignored. It never reduced "
            "memory (it sampled the list after it was already built) and it applied only to "
            "the mean, so the mean and the percentiles described different populations. "
            "Percentiles now come from a fixed-memory histogram over every message."
        )

    config = KafkaConfig(
        consumer_config_file=os.getenv("CONSUMER_CONFIG_FILE"),
        producer_config_file=os.getenv("PRODUCER_CONFIG_FILE"),
        input_topic=os.getenv("INPUT_TOPIC"),
        group_id=os.getenv("GROUP_ID"),
        run_interval=int(os.getenv("RUN_INTERVAL", "0")),
        t1=os.getenv("T1"),
        t2=os.getenv("T2"),
        output_type=os.getenv("CONSUMER_OUTPUT"),
        local_filepath=os.getenv("RESULT_DUMP_LOCAL_FILEPATH"),
        output_topic=os.getenv("OUTPUT_TOPIC"),
        value_deserializer=os.getenv("VALUE_DESERIALIZER"),
        key_deserializer=os.getenv("KEY_DESERIALIZER"),
        date_time_format=os.getenv("DATE_TIME_FORMAT"),
        protobuf_message_name=os.getenv("PROTOBUF_MESSAGE_NAME"),
        t1_unit=os.getenv("T1_UNIT", "ms").lower(),
        t1_timezone=os.getenv("T1_TIMEZONE", "utc").lower(),
    )
    validate_config(config)
    return config

def _read_properties(config_file: str) -> dict:
    """Read a java-style properties file into a dict."""
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf

def read_ccloud_config(config_file: str) -> dict:
    """Read and parse Kafka configuration file."""
    try:
        conf = _read_properties(config_file)
        conf.pop('schema.registry.url', None)
        conf.pop('basic.auth.user.info', None)
        conf.pop('basic.auth.credentials.source', None)
        return conf
    except Exception as e:
        logger.error(f"Error reading config file {config_file}: {str(e)}")
        raise

def read_sr_config(config_file: str) -> dict:
    """Read and parse Schema Registry configuration."""
    try:
        conf = _read_properties(config_file)
        if 'schema.registry.url' not in conf:
            raise ValueError(
                f"schema.registry.url is required in {config_file} when using an "
                "Avro or JSON Schema deserializer"
            )
        sr_config = {'url': conf['schema.registry.url']}
        # Optional: a Schema Registry without authentication is normal locally.
        if 'basic.auth.user.info' in conf:
            sr_config['basic.auth.user.info'] = conf['basic.auth.user.info']
        return sr_config
    except Exception as e:
        logger.error(f"Error reading schema registry config file {config_file}: {str(e)}")
        raise
