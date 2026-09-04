"""Shared factories. Nothing here talks to a broker or a Schema Registry."""

import pytest

from src.config.config_manager import KafkaConfig

DEFAULT_CONFIG = dict(
    consumer_config_file="client.properties",
    producer_config_file=None,
    input_topic="test-topic",
    group_id="test-group",
    enable_sampling=False,
    run_interval=120,
    t1="IngestionTime",
    t2="consumerWallClockTime",
    output_type="localFileDump",
    local_filepath="out.csv",
    output_topic=None,
    value_deserializer="StringDeserializer",
    key_deserializer="StringDeserializer",
    date_time_format="epoch",
    t1_unit="ms",
    t1_timezone="utc",
)


class FakeMessage:
    """Minimal stand-in for confluent_kafka.Message."""

    def __init__(self, value=None, key=None, timestamp_ms=0, timestamp_type=1, error=None):
        self._value = value
        self._key = key
        self._timestamp = (timestamp_type, timestamp_ms)
        self._error = error

    def value(self):
        return self._value

    def key(self):
        return self._key

    def timestamp(self):
        return self._timestamp

    def error(self):
        return self._error

    def topic(self):
        return "test-topic"


class FakeProcessor:
    """Stands in for MessageProcessor when only the results matter."""

    def __init__(self, latencies):
        self.latency_array = list(latencies)
        self.count = len(self.latency_array)
        self.diagnostics_logged = False

    def log_diagnostics(self):
        self.diagnostics_logged = True


@pytest.fixture
def make_config(tmp_path):
    """A valid config whose output path is always inside the test's tmp dir.

    Tests that reach the file output must not write anywhere else: a relative
    path lands in the repository, and at this commit file_output still joins
    onto /app/data, which is a PermissionError on Linux.
    """
    def _make(**overrides):
        defaults = {**DEFAULT_CONFIG, 'local_filepath': str(tmp_path / "out.csv")}
        return KafkaConfig(**{**defaults, **overrides})

    return _make


@pytest.fixture
def make_message():
    return FakeMessage


@pytest.fixture
def make_processor():
    return FakeProcessor
