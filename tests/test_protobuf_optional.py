"""Protobuf is an optional install; absence must degrade gracefully.

These tests always run, including in an environment with none of the protobuf
packages, because what they check is precisely that the tool works without
them and explains itself if a protobuf deserializer is configured anyway.
"""

import sys

import pytest

from src.config.config_manager import VALID_DESERIALIZERS, create_kafka_config
from src.core.protobuf_schema import ProtobufSchemaError, build_protobuf_deserializer

ENV = {
    "INPUT_TOPIC": "test-topic",
    "GROUP_ID": "test-group",
    "RUN_INTERVAL": "120",
    "T1": "value.produced_at",
    "T2": "consumerWallClockTime",
    "CONSUMER_OUTPUT": "localFileDump",
    "RESULT_DUMP_LOCAL_FILEPATH": "out.csv",
    "VALUE_DESERIALIZER": "ProtobufDeserializer",
    "KEY_DESERIALIZER": "StringDeserializer",
    "DATE_TIME_FORMAT": "epoch",
}


@pytest.fixture
def env(monkeypatch, tmp_path):
    config_file = tmp_path / "client.properties"
    config_file.write_text("bootstrap.servers=localhost:9092\n")
    for key in list(ENV) + ["PROTOBUF_MESSAGE_NAME", "T1_UNIT", "T1_TIMEZONE",
                            "PRODUCER_CONFIG_FILE", "OUTPUT_TOPIC"]:
        monkeypatch.delenv(key, raising=False)
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("CONSUMER_CONFIG_FILE", str(config_file))
    return monkeypatch


# -- the tool imports and configures without the optional packages ----------

def test_importing_the_application_does_not_need_protobuf():
    """No protobuf import at module scope anywhere on the default path."""
    import main  # noqa: F401
    import src.core.message_processor  # noqa: F401


def test_protobuf_is_an_accepted_deserializer(env):
    assert "ProtobufDeserializer" in VALID_DESERIALIZERS
    assert create_kafka_config().value_deserializer == "ProtobufDeserializer"


def test_the_message_name_override_is_read(env):
    env.setenv("PROTOBUF_MESSAGE_NAME", "demo.Event")
    assert create_kafka_config().protobuf_message_name == "demo.Event"


def test_the_message_name_defaults_to_none(env):
    assert create_kafka_config().protobuf_message_name is None


def test_an_invalid_deserializer_lists_protobuf_among_the_options(env):
    env.setenv("VALUE_DESERIALIZER", "ThriftDeserializer")
    with pytest.raises(ValueError, match="ProtobufDeserializer"):
        create_kafka_config()


# -- missing packages are explained, not a traceback ------------------------

@pytest.mark.parametrize(
    "absent",
    ["grpc_tools", "confluent_kafka.schema_registry.protobuf"],
)
def test_missing_packages_point_at_the_install_command(monkeypatch, absent):
    """Setting None in sys.modules makes the import fail the way absence does."""
    monkeypatch.setitem(sys.modules, absent, None)

    with pytest.raises(ProtobufSchemaError, match="requirements-protobuf.txt"):
        build_protobuf_deserializer(schema=None)


def test_the_error_names_the_setting_that_triggered_it(monkeypatch):
    monkeypatch.setitem(sys.modules, "grpc_tools", None)

    with pytest.raises(ProtobufSchemaError, match="ProtobufDeserializer"):
        build_protobuf_deserializer(schema=None)
