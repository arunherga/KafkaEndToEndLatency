"""Protobuf support: schema comes from Schema Registry, class is built at runtime.

Skipped entirely when the optional protobuf packages are absent, which is the
point of them being optional. CI runs this file in a job that installs them.
"""

import json

import pytest

pytest.importorskip("grpc_tools", reason="optional: pip install -r requirements-protobuf.txt")
pytest.importorskip("google.protobuf", reason="optional: pip install -r requirements-protobuf.txt")

from confluent_kafka.schema_registry import Schema  # noqa: E402
from confluent_kafka.schema_registry.serde import SchemaId  # noqa: E402
from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: E402

from src.core.message_processor import MessageProcessor  # noqa: E402
from src.core.protobuf_schema import (  # noqa: E402
    ProtobufSchemaError,
    build_message_class,
    build_protobuf_deserializer,
    to_dict,
)

SIMPLE = '''
syntax = "proto3";
package demo;
message Event {
  string id = 1;
  int64 produced_at = 2;
  Header header = 3;
}
message Header { int64 emitted_at = 1; }
'''

WELL_KNOWN = '''
syntax = "proto3";
package demo;
import "google/protobuf/timestamp.proto";
message Event {
  string id = 1;
  google.protobuf.Timestamp produced_at = 2;
}
'''

WITH_IMPORT = '''
syntax = "proto3";
package demo;
import "common/header.proto";
message Event {
  string id = 1;
  common.Header header = 2;
}
'''

HEADER = '''
syntax = "proto3";
package common;
message Header { int64 produced_at = 1; }
'''


def proto_schema(schema_str, references=None):
    return Schema(schema_str, schema_type="PROTOBUF", references=references or [])


class FakeRegistry:
    """Returns registered schemas by subject, like Schema Registry does."""

    def __init__(self, by_subject=None, latest=None):
        self._by_subject = by_subject or {}
        self._latest = latest

    def get_version(self, subject, version):
        class Registered:
            schema = self._by_subject[subject]
        return Registered()

    def get_latest_version(self, subject):
        class Version:
            schema_id = 1
        return Version()

    def get_schema(self, schema_id, *args, **kwargs):
        return self._latest


# -- building a class from a registered schema ------------------------------

def test_a_class_is_built_from_the_registered_schema():
    """No pre-generated _pb2.py: the .proto comes from the registry."""
    Event = build_message_class(proto_schema(SIMPLE))

    message = Event(id="abc", produced_at=1788409800000)
    decoded = Event()
    decoded.ParseFromString(message.SerializeToString())

    assert decoded.id == "abc"
    assert decoded.produced_at == 1788409800000


def test_the_first_declared_message_is_used_by_default():
    """Confluent's default message index (0) refers to the first message."""
    Event = build_message_class(proto_schema(SIMPLE))
    assert Event.DESCRIPTOR.full_name == "demo.Event"


def test_a_specific_message_can_be_selected():
    Header = build_message_class(proto_schema(SIMPLE), message_name="demo.Header")
    assert Header.DESCRIPTOR.full_name == "demo.Header"


def test_an_unknown_message_name_lists_what_is_available():
    with pytest.raises(ProtobufSchemaError, match="demo.Event, demo.Header"):
        build_message_class(proto_schema(SIMPLE), message_name="demo.Nope")


def test_well_known_types_resolve_without_extra_setup():
    """google/protobuf/*.proto ships with grpc_tools; imports must just work."""
    Event = build_message_class(proto_schema(WELL_KNOWN))
    assert Event.DESCRIPTOR.fields_by_name["produced_at"].message_type.full_name == (
        "google.protobuf.Timestamp"
    )


def test_imports_of_other_registered_schemas_are_resolved():
    """A .proto may import another subject; protoc needs it written to disk."""
    registry = FakeRegistry(by_subject={"common-header": proto_schema(HEADER)})
    schema = proto_schema(
        WITH_IMPORT,
        references=[type("Ref", (), {"name": "common/header.proto",
                                     "subject": "common-header", "version": 1})()],
    )

    Event = build_message_class(schema, schema_registry_client=registry)

    assert Event.DESCRIPTOR.fields_by_name["header"].message_type.full_name == "common.Header"


def test_an_unresolvable_import_is_reported():
    schema = proto_schema(
        WITH_IMPORT,
        references=[type("Ref", (), {"name": "common/header.proto",
                                     "subject": "common-header", "version": 1})()],
    )
    with pytest.raises(ProtobufSchemaError, match="needs a Schema Registry client"):
        build_message_class(schema, schema_registry_client=None)


def test_a_malformed_schema_is_reported_clearly():
    with pytest.raises(ProtobufSchemaError, match="protoc failed"):
        build_message_class(proto_schema("this is not a .proto file"))


def test_a_schema_with_no_messages_is_reported():
    with pytest.raises(ProtobufSchemaError, match="no message types"):
        build_message_class(proto_schema('syntax = "proto3"; package demo;'))


# -- conversion to mappings -------------------------------------------------

def test_decoded_messages_become_plain_dicts_with_proto_field_names():
    """T1=value.produced_at must mean the field as declared in the .proto."""
    Event = build_message_class(proto_schema(SIMPLE))
    message = Event(id="abc", produced_at=1788409800000)
    message.header.emitted_at = 1788409800123

    payload = to_dict(message)

    assert payload["id"] == "abc"
    assert payload["header"]["emitted_at"] == "1788409800123"


def test_int64_fields_arrive_as_strings_and_are_still_usable():
    """Protobuf's JSON mapping renders 64-bit ints as strings; the epoch
    conversion accepts that, so this is a supported shape rather than a bug."""
    from src.core.timestamps import epoch_to_millis

    Event = build_message_class(proto_schema(SIMPLE))
    payload = to_dict(Event(produced_at=1788409800000))

    assert isinstance(payload["produced_at"], str)
    assert epoch_to_millis(payload["produced_at"], "ms") == 1788409800000.0


# -- the wire format --------------------------------------------------------

def confluent_framed(message, schema_id=1, message_indexes=None):
    """Confluent framing: magic byte, schema id, message index array, payload."""
    prefix = SchemaId("PROTOBUF", schema_id, None, message_indexes or [0]).id_to_bytes()
    return prefix + message.SerializeToString()


def test_a_confluent_framed_message_decodes_to_a_dict():
    deserialize = build_protobuf_deserializer(proto_schema(SIMPLE))
    Event = build_message_class(proto_schema(SIMPLE))

    raw = confluent_framed(Event(id="abc", produced_at=1788409800000))
    payload = deserialize(raw, SerializationContext("test-topic", MessageField.VALUE))

    assert payload["id"] == "abc"
    assert payload["produced_at"] == "1788409800000"


# -- end to end through MessageProcessor ------------------------------------

@pytest.fixture
def protobuf_processor(monkeypatch, make_config):
    """A MessageProcessor wired to a fake registry serving a protobuf schema."""
    import src.core.message_processor as mp

    schema = proto_schema(SIMPLE)
    monkeypatch.setattr(mp, "read_sr_config", lambda _path: {"url": "http://fake"})
    monkeypatch.setattr(mp, "SchemaRegistryClient", lambda _conf: FakeRegistry(latest=schema))

    def _build(**overrides):
        config = make_config(**{"value_deserializer": "ProtobufDeserializer",
                                "t1": "value.produced_at", **overrides})
        return MessageProcessor(config)

    return _build


def test_a_timestamp_is_read_from_a_protobuf_value(protobuf_processor, make_message):
    processor = protobuf_processor()
    Event = build_message_class(proto_schema(SIMPLE))
    raw = confluent_framed(Event(id="abc", produced_at=1788409800000))

    assert processor._extract_time1(make_message(value=raw)) == 1788409800000.0


def test_a_nested_protobuf_timestamp_is_read(protobuf_processor, make_message):
    processor = protobuf_processor(t1="value.header.emitted_at")
    Event = build_message_class(proto_schema(SIMPLE))
    message = Event(id="abc")
    message.header.emitted_at = 1788409800123
    raw = confluent_framed(message)

    assert processor._extract_time1(make_message(value=raw)) == 1788409800123.0


def test_a_missing_protobuf_field_is_skipped_not_guessed(protobuf_processor, make_message):
    processor = protobuf_processor(t1="value.not_a_field")
    Event = build_message_class(proto_schema(SIMPLE))
    raw = confluent_framed(Event(id="abc", produced_at=1788409800000))

    assert processor._extract_time1(make_message(value=raw)) is None
    assert processor.skipped["unparsable_t1"] == 1


def test_a_non_protobuf_payload_is_skipped(protobuf_processor, make_message):
    processor = protobuf_processor()

    assert processor.process_message(make_message(value=json.dumps({"a": 1}).encode())) is None
    assert processor.skipped["undeserializable_value"] == 1


def test_the_selected_message_name_reaches_the_builder(protobuf_processor, make_message):
    """PROTOBUF_MESSAGE_NAME must actually change which message is decoded."""
    processor = protobuf_processor(t1="value.emitted_at",
                                   protobuf_message_name="demo.Header")
    Header = build_message_class(proto_schema(SIMPLE), message_name="demo.Header")
    raw = confluent_framed(Header(emitted_at=1788409800123))

    assert processor._extract_time1(make_message(value=raw)) == 1788409800123.0
