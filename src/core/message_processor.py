import json
import logging
import time
from collections import Counter
from typing import Any, Optional

from confluent_kafka import (
    TIMESTAMP_CREATE_TIME,
    TIMESTAMP_LOG_APPEND_TIME,
    TIMESTAMP_NOT_AVAILABLE,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from src.config.config_manager import KafkaConfig, read_sr_config
from src.core.timestamps import (
    TimestampError,
    extract_field,
    split_timestamp_spec,
    to_millis,
)

logger = logging.getLogger(__name__)

# Deserializers that take raw bytes of JSON, with no Schema Registry involved.
PLAIN_JSON_DESERIALIZERS = ('StringDeserializer', 'JSONDeserializer')
# Deserializers backed by Schema Registry.
REGISTRY_DESERIALIZERS = ('AvroDeserializer', 'JSONSchemaDeserializer')

TIMESTAMP_TYPE_NAMES = {
    TIMESTAMP_NOT_AVAILABLE: 'not available',
    TIMESTAMP_CREATE_TIME: 'CreateTime (producer clock)',
    TIMESTAMP_LOG_APPEND_TIME: 'LogAppendTime (broker clock)',
}


class MessageProcessor:
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.latency_array = []
        self.count = 0

        # A run that silently drops most of its messages should say so, rather
        # than reporting confident percentiles over whatever happened to survive.
        self.skipped = Counter()
        self.consumer_errors = Counter()
        self.timestamp_types = Counter()
        self.negative_latencies = 0
        self._logged_reasons = set()

        self.t1_source, self.t1_path = split_timestamp_spec(config.t1)
        self._setup_deserializers()

    # -- setup -------------------------------------------------------------

    def _setup_deserializers(self):
        """Set up the appropriate deserializers based on configuration."""
        self.schema_registry = None
        self.value_deserializer = None
        self.key_deserializer = None

        needed = {'value': self.config.value_deserializer}
        # The key is only deserialized when T1 actually reads a field from it.
        if self.t1_source == 'key':
            needed['key'] = self.config.key_deserializer

        if not any(name in REGISTRY_DESERIALIZERS for name in needed.values()):
            return

        self.schema_registry = SchemaRegistryClient(read_sr_config(self.config.consumer_config_file))
        for part, name in needed.items():
            if name in REGISTRY_DESERIALIZERS:
                setattr(self, f'{part}_deserializer', self._build_registry_deserializer(part, name))

    def _build_registry_deserializer(self, part: str, name: str):
        if name == 'AvroDeserializer':
            # No schema_str: let the client resolve each message against the
            # schema it was actually written with. Pinning the reader schema to
            # the latest version breaks on messages written against an older one.
            return AvroDeserializer(schema_registry_client=self.schema_registry)

        subject = f'{self.config.input_topic}-{part}'
        schema = self.schema_registry.get_schema(
            self.schema_registry.get_latest_version(subject).schema_id
        )
        return JSONDeserializer(schema_str=schema.schema_str)

    # -- per message -------------------------------------------------------

    def process_message(self, msg) -> Optional[float]:
        """Process a Kafka message and calculate latency."""
        # Read the consumer clock before any deserialization work, so the cost
        # of decoding this message is not counted as part of its own latency.
        received_at_ms = time.time() * 1000

        try:
            if msg is None:
                return None

            error = msg.error()
            if error is not None:
                self.consumer_errors[str(error)] += 1
                self._skip('consumer_error', f"Consumer error: {error}")
                return None

            time1 = self._extract_time1(msg)
            if time1 is None:
                return None

            time2 = self._extract_time2(msg, received_at_ms)
            if time2 is None:
                return None

            latency = time2 - time1
            if latency < 0:
                # Almost always clock skew between the two hosts rather than a
                # real negative latency. Kept in the sample, but counted.
                self.negative_latencies += 1

            self.latency_array.append(latency)
            self.count += 1
            return latency

        except Exception as e:
            self._skip('unexpected_error', f"Error processing message: {str(e)}")
            return None

    def _extract_time1(self, msg) -> Optional[float]:
        """Extract the first timestamp from the message, in epoch milliseconds."""
        if self.t1_source == 'ingestion':
            return self._broker_timestamp_ms(msg)

        payload = self._deserialize(msg, self.t1_source)
        if payload is None:
            return None

        try:
            raw = extract_field(payload, self.t1_path)
            return to_millis(
                raw,
                self.config.date_time_format,
                self.config.t1_unit,
                self.config.t1_timezone,
            )
        except TimestampError as e:
            self._skip('unparsable_t1', f"Could not read T1 from {self.config.t1}: {e}")
            return None

    def _extract_time2(self, msg, received_at_ms: float) -> Optional[float]:
        """Extract the second timestamp, in epoch milliseconds."""
        if self.config.t2 == 'IngestionTime':
            return self._broker_timestamp_ms(msg)
        if self.config.t2 == 'consumerWallClockTime':
            return received_at_ms
        self._skip('unknown_t2', f"Unknown T2 {self.config.t2!r}")
        return None

    def _broker_timestamp_ms(self, msg) -> Optional[float]:
        """The message's own timestamp, if the broker actually recorded one."""
        timestamp_type, value = msg.timestamp()
        self.timestamp_types[timestamp_type] += 1

        if timestamp_type == TIMESTAMP_NOT_AVAILABLE or value < 0:
            self._skip(
                'no_message_timestamp',
                "Message has no broker timestamp; check the topic's message.timestamp.type",
            )
            return None
        return float(value)

    def _deserialize(self, msg, part: str) -> Optional[Any]:
        """Deserialize the key or the value with the configured deserializer."""
        raw = msg.key() if part == 'key' else msg.value()
        if raw is None:
            self._skip(f'empty_{part}', f"Message has no {part}")
            return None

        name = self.config.key_deserializer if part == 'key' else self.config.value_deserializer
        try:
            if name in PLAIN_JSON_DESERIALIZERS:
                return json.loads(raw.decode('utf-8'))

            deserializer = self.key_deserializer if part == 'key' else self.value_deserializer
            if deserializer is None:
                self._skip(f'no_{part}_deserializer', f"No deserializer configured for the message {part}")
                return None

            field = MessageField.KEY if part == 'key' else MessageField.VALUE
            return deserializer(raw, SerializationContext(msg.topic(), field))
        except Exception as e:
            self._skip(f'undeserializable_{part}', f"Error deserializing message {part}: {str(e)}")
            return None

    def _skip(self, reason: str, message: str) -> None:
        """Count a skipped message, logging each distinct reason only once.

        A malformed topic would otherwise emit one log line per message.
        """
        self.skipped[reason] += 1
        if reason not in self._logged_reasons:
            self._logged_reasons.add(reason)
            logger.warning(f"{message} (further occurrences counted, not logged)")

    # -- reporting ---------------------------------------------------------

    @property
    def skipped_total(self) -> int:
        return sum(self.skipped.values())

    def log_diagnostics(self) -> None:
        """Report everything that affects how much to trust the measurements."""
        seen = self.count + self.skipped_total
        logger.info(f"Messages seen: {seen} (measured: {self.count}, skipped: {self.skipped_total})")

        for reason, count in self.skipped.most_common():
            logger.warning(f"  skipped {count} message(s): {reason}")

        for error, count in self.consumer_errors.most_common(5):
            logger.warning(f"  consumer error x{count}: {error}")

        for timestamp_type, count in self.timestamp_types.most_common():
            name = TIMESTAMP_TYPE_NAMES.get(timestamp_type, f'unknown ({timestamp_type})')
            logger.info(f"  message timestamp type: {name} x{count}")

        if self.negative_latencies:
            share = self.negative_latencies / self.count * 100 if self.count else 0
            logger.warning(
                f"{self.negative_latencies} of {self.count} latencies were negative ({share:.1f}%). "
                "T1 and T2 are read from different hosts, so this normally means their clocks "
                "disagree; the reported percentiles are only as good as that clock sync."
            )
