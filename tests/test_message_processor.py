"""Behaviour of MessageProcessor once every timestamp is normalised to epoch ms."""

import json
import time

import pytest
from confluent_kafka import (
    TIMESTAMP_CREATE_TIME,
    TIMESTAMP_LOG_APPEND_TIME,
    TIMESTAMP_NOT_AVAILABLE,
)

from src.core.message_processor import MessageProcessor


def json_bytes(payload):
    return json.dumps(payload).encode("utf-8")


# -- epoch units ------------------------------------------------------------

@pytest.mark.parametrize("unit, produced_at", [("s", 1_600_000_000), ("ms", 1_600_000_000_000)])
def test_t1_unit_is_honoured_for_epoch_fields(make_config, make_message, unit, produced_at):
    """An epoch field carries no unit of its own, so T1_UNIT has to supply it."""
    config = make_config(t1="value.produced_at", t1_unit=unit)
    processor = MessageProcessor(config)
    msg = make_message(value=json_bytes({"produced_at": produced_at}))

    assert processor._extract_time1(msg) == 1_600_000_000_000.0


def test_seconds_read_as_milliseconds_would_be_absurd(make_config, make_message):
    """Guards the silent 1000x error: the same field under the wrong unit."""
    msg = make_message(value=json_bytes({"produced_at": 1_600_000_000}))

    correct = MessageProcessor(make_config(t1="value.produced_at", t1_unit="s"))._extract_time1(msg)
    wrong = MessageProcessor(make_config(t1="value.produced_at", t1_unit="ms"))._extract_time1(msg)

    assert correct / wrong == 1000


# -- key vs value -----------------------------------------------------------

def test_t1_can_read_a_field_from_the_key(make_config, make_message):
    """key.* was accepted by validation but silently read from the value."""
    config = make_config(t1="key.produced_at")
    processor = MessageProcessor(config)
    msg = make_message(
        key=json_bytes({"produced_at": 1_600_000_000_000}),
        value=json_bytes({"produced_at": 999}),
    )

    assert processor._extract_time1(msg) == 1_600_000_000_000.0


def test_nested_value_fields_are_supported(make_config, make_message):
    """t1.split('.')[1] could only ever reach a top-level field."""
    config = make_config(t1="value.header.produced_at")
    processor = MessageProcessor(config)
    msg = make_message(value=json_bytes({"header": {"produced_at": 1_600_000_000_000}}))

    assert processor._extract_time1(msg) == 1_600_000_000_000.0


def test_json_deserializer_is_implemented(make_config, make_message):
    """JSONDeserializer passed validation but produced None for every message."""
    config = make_config(t1="value.produced_at", value_deserializer="JSONDeserializer")
    processor = MessageProcessor(config)
    msg = make_message(value=json_bytes({"produced_at": 1_600_000_000_000}))

    assert processor._extract_time1(msg) == 1_600_000_000_000.0


# -- broker timestamps ------------------------------------------------------

def test_missing_broker_timestamp_is_skipped_not_treated_as_minus_one(make_config, make_message):
    """msg.timestamp() returns (NOT_AVAILABLE, -1), which used to be used as a real value."""
    processor = MessageProcessor(make_config(t1="IngestionTime"))
    msg = make_message(timestamp_ms=-1, timestamp_type=TIMESTAMP_NOT_AVAILABLE)

    assert processor.process_message(msg) is None
    assert processor.count == 0
    assert processor.skipped["no_message_timestamp"] == 1


def test_observed_timestamp_types_are_recorded(make_config, make_message):
    """CreateTime and LogAppendTime measure different things; the run should say which."""
    processor = MessageProcessor(make_config(t1="IngestionTime"))
    processor.process_message(make_message(timestamp_ms=1_600_000_000_000, timestamp_type=TIMESTAMP_CREATE_TIME))
    processor.process_message(
        make_message(timestamp_ms=1_600_000_000_000, timestamp_type=TIMESTAMP_LOG_APPEND_TIME)
    )

    assert processor.timestamp_types[TIMESTAMP_CREATE_TIME] == 1
    assert processor.timestamp_types[TIMESTAMP_LOG_APPEND_TIME] == 1


# -- diagnostics ------------------------------------------------------------

def test_consumer_errors_are_counted_not_silently_dropped(make_config, make_message):
    processor = MessageProcessor(make_config())
    processor.process_message(make_message(error="broker transport failure"))

    assert processor.count == 0
    assert processor.skipped["consumer_error"] == 1
    assert processor.consumer_errors["broker transport failure"] == 1


def test_unparsable_timestamps_are_counted(make_config, make_message):
    processor = MessageProcessor(make_config(t1="value.produced_at"))
    for _ in range(5):
        processor.process_message(make_message(value=json_bytes({"produced_at": "yesterday"})))

    assert processor.count == 0
    assert processor.skipped["unparsable_t1"] == 5


def test_each_skip_reason_is_logged_only_once(make_config, make_message, caplog):
    """A malformed topic would otherwise emit one warning per message."""
    processor = MessageProcessor(make_config(t1="value.produced_at"))
    with caplog.at_level("WARNING"):
        for _ in range(50):
            processor.process_message(make_message(value=json_bytes({"produced_at": "yesterday"})))

    assert processor.skipped["unparsable_t1"] == 50
    assert caplog.text.count("Could not read T1") == 1


def test_negative_latencies_are_counted_and_warned_about(make_config, make_message, caplog):
    """Clock skew between the two hosts, not a real negative latency."""
    future_ms = (time.time() + 3600) * 1000
    processor = MessageProcessor(make_config(t1="value.produced_at"))
    processor.process_message(make_message(value=json_bytes({"produced_at": future_ms})))

    assert processor.count == 1
    assert processor.negative_latencies == 1

    with caplog.at_level("WARNING"):
        processor.log_diagnostics()
    assert "clocks" in caplog.text


def test_skipped_messages_are_reported_in_the_summary(make_config, make_message, caplog):
    processor = MessageProcessor(make_config(t1="value.produced_at"))
    processor.process_message(make_message(value=json_bytes({"produced_at": 1_600_000_000_000})))
    processor.process_message(make_message(value=json_bytes({"wrong_field": 1})))

    with caplog.at_level("INFO"):
        processor.log_diagnostics()

    assert "Messages seen: 2 (measured: 1, skipped: 1)" in caplog.text
    assert processor.skipped_total == 1


# -- measurement hygiene ----------------------------------------------------

def test_t2_is_captured_before_deserialization(make_config, make_message, monkeypatch):
    """The cost of decoding a message must not land inside its own latency."""
    config = make_config(t1="value.produced_at")
    processor = MessageProcessor(config)

    clock = iter([1_000_000.0, 2_000_000.0])
    monkeypatch.setattr(time, "time", lambda: next(clock))

    msg = make_message(value=json_bytes({"produced_at": 1_000_000_000.0}))
    latency = processor.process_message(msg)

    # The first (and only) clock reading is taken up front, so the second value
    # in the iterator is never consumed by process_message.
    assert latency == 1_000_000.0 * 1000 - 1_000_000_000.0


def test_missing_value_is_skipped(make_config, make_message):
    processor = MessageProcessor(make_config(t1="value.produced_at"))

    assert processor.process_message(make_message(value=None)) is None
    assert processor.skipped["empty_value"] == 1
