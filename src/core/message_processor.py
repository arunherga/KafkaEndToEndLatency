import time
from datetime import datetime
import json
import logging
from typing import Optional, Dict, Any, List
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.avro import AvroDeserializer

from src.config.config_manager import KafkaConfig, read_sr_config

logger = logging.getLogger(__name__)

class MessageProcessor:
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.latency_array = []
        self.count = 0
        self._setup_deserializers()
        
    def _setup_deserializers(self):
        """Set up the appropriate deserializers based on configuration."""
        self.schema_registry = None
        self.value_deserializer = None
        self.key_deserializer = None
        
        if self.config.value_deserializer in ['AvroDeserializer', 'JSONSchemaDeserializer']:
            self.schema_registry = SchemaRegistryClient(read_sr_config(self.config.consumer_config_file))
            schema = self.schema_registry.get_schema(
                self.schema_registry.get_latest_version(f'{self.config.input_topic}-value').schema_id
            )
            
            if self.config.value_deserializer == 'JSONSchemaDeserializer':
                self.value_deserializer = JSONDeserializer(schema_str=schema.schema_str)
            else:
                self.value_deserializer = AvroDeserializer(
                    schema_registry_client=self.schema_registry,
                    schema_str=schema.schema_str
                )

    def process_message(self, msg) -> Optional[float]:
        """Process a Kafka message and calculate latency."""
        try:
            if msg is None or msg.error():
                return None
                
            time1 = self._extract_time1(msg)
            time2 = self._extract_time2(msg)
            
            if time1 is None or time2 is None:
                return None
                
            latency = time2 - time1
            self.latency_array.append(latency)
            self.count += 1
            return latency
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            return None
            
    def _extract_time1(self, msg) -> Optional[float]:
        """Extract the first timestamp from the message."""
        try:
            if self.config.t1 == "IngestionTime":
                return int(msg.timestamp()[1])
                
            message_value = self._get_message_value(msg)
            if message_value is None:
                return None
                
            field = self.config.t1.split('.')[1]
            if self.config.date_time_format == "epoch":
                return message_value[field]
            else:
                time_obj = datetime.strptime(message_value[field], self.config.date_time_format)
                return time_obj.timestamp()
                
        except Exception as e:
            logger.error(f"Error extracting time1: {str(e)}")
            return None
            
    def _extract_time2(self, msg) -> Optional[float]:
        """Extract the second timestamp from the message."""
        try:
            if self.config.t2 == 'IngestionTime':
                return int(msg.timestamp()[1])
            elif self.config.t2 == 'consumerWallClockTime':
                return time.time() * 1000
            return None
        except Exception as e:
            logger.error(f"Error extracting time2: {str(e)}")
            return None
            
    def _get_message_value(self, msg) -> Optional[Dict[str, Any]]:
        """Get the message value using the appropriate deserializer."""
        try:
            if self.config.value_deserializer == 'StringDeserializer':
                return json.loads(msg.value().decode('utf-8'))
            elif self.config.value_deserializer in ['AvroDeserializer', 'JSONSchemaDeserializer']:
                return self.value_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            return None
        except Exception as e:
            logger.error(f"Error getting message value: {str(e)}")
            return None 