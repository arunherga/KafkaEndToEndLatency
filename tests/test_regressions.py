"""Regression tests for the crashes and the unit bug fixed alongside them.

Every test here fails on the code as it was before this change:

  * process_results() raised NameError whenever ENABLE_SAMPLING=True, because
    random was never imported -- and True is the default in docker-compose.yml.
  * process_results() raised ZeroDivisionError whenever a run captured no
    messages, or captured too few for the 30% sample to round above zero.
  * GROUP_ID was read into the config object but never handed to the Consumer.
  * The strptime path of _extract_time1 returned seconds while _extract_time2
    returns milliseconds, inflating every latency by ~1.8e12 ms.
"""

import json
from datetime import datetime
from types import SimpleNamespace

import pytest

import main
from src.config.config_manager import KafkaConfig
from src.core.message_processor import MessageProcessor


def make_config(**overrides):
    """A valid config, defaulting to the cheapest path (no output, no schema registry)."""
    base = dict(
        consumer_config_file="client.properties",
        producer_config_file=None,
        input_topic="test-topic",
        group_id="test-group",
        enable_sampling=False,
        run_interval=120,
        t1="IngestionTime",
        t2="consumerWallClockTime",
        output_type="noOutput",
        local_filepath=None,
        output_topic=None,
        value_deserializer="StringDeserializer",
        key_deserializer="StringDeserializer",
        date_time_format="epoch",
    )
    base.update(overrides)
    return KafkaConfig(**base)


def make_processor(latencies):
    """Stand-in for MessageProcessor: process_results reads only these two attributes."""
    latencies = list(latencies)
    return SimpleNamespace(latency_array=latencies, count=len(latencies))


class FakeMessage:
    """Minimal stand-in for confluent_kafka.Message."""

    def __init__(self, value, timestamp_ms=0):
        self._value = value
        self._timestamp_ms = timestamp_ms

    def value(self):
        return self._value

    def timestamp(self):
        return (1, self._timestamp_ms)

    def error(self):
        return None

    def topic(self):
        return "test-topic"


# --------------------------------------------------------------------------
# process_results
# --------------------------------------------------------------------------

def test_sampling_enabled_reports_results(caplog):
    """ENABLE_SAMPLING=True is the shipped default; it used to raise NameError."""
    with caplog.at_level("INFO"):
        main.process_results(make_processor(range(100)), make_config(enable_sampling=True))

    assert "Average Latency in ms" in caplog.text
    assert "Number of message sampled(sampling enabled): 30" in caplog.text


def test_sampling_on_a_very_short_run_does_not_divide_by_zero(caplog):
    """3 messages * 0.3 rounds down to a sample of 0, which used to be a ZeroDivisionError."""
    with caplog.at_level("INFO"):
        main.process_results(make_processor([10, 20, 30]), make_config(enable_sampling=True))

    assert "Average Latency in ms" in caplog.text


def test_no_messages_warns_instead_of_crashing(caplog):
    """A quiet or misnamed topic is a normal outcome, not a ZeroDivisionError."""
    with caplog.at_level("WARNING"):
        main.process_results(make_processor([]), make_config())

    assert "no latency results" in caplog.text
    assert "test-topic" in caplog.text


def test_results_are_still_computed_without_sampling(caplog):
    with caplog.at_level("INFO"):
        main.process_results(make_processor([100, 200, 300]), make_config())

    assert "Average Latency in ms: 200" in caplog.text


# --------------------------------------------------------------------------
# GROUP_ID wiring
# --------------------------------------------------------------------------

def _set_env(monkeypatch, config_path, **overrides):
    env = {
        "CONSUMER_CONFIG_FILE": str(config_path),
        "INPUT_TOPIC": "test-topic",
        "GROUP_ID": "my-profiler-group",
        "RUN_INTERVAL": "0",
        "T1": "IngestionTime",
        "T2": "consumerWallClockTime",
        "CONSUMER_OUTPUT": "localFileDump",
        "RESULT_DUMP_LOCAL_FILEPATH": "out.csv",
        "VALUE_DESERIALIZER": "StringDeserializer",
        "KEY_DESERIALIZER": "StringDeserializer",
        "DATE_TIME_FORMAT": "epoch",
        "ENABLE_SAMPLING": "False",
    }
    env.update(overrides)
    for key, value in env.items():
        monkeypatch.setenv(key, value)


def _fake_consumer_class(captured):
    class FakeConsumer:
        def __init__(self, conf):
            captured["conf"] = conf

        def subscribe(self, topics):
            captured["topics"] = topics

        def poll(self, timeout):
            return None

        def close(self):
            captured["closed"] = True

    return FakeConsumer


def test_group_id_env_var_reaches_the_consumer(monkeypatch, tmp_path):
    """GROUP_ID used to be documented, validated and then silently ignored."""
    config_file = tmp_path / "client.properties"
    config_file.write_text("bootstrap.servers=localhost:9092\n")

    captured = {}
    monkeypatch.setattr(main, "Consumer", _fake_consumer_class(captured))
    monkeypatch.setattr(main, "MessageProcessor", lambda config: make_processor([]))
    _set_env(monkeypatch, config_file)

    main.main()

    assert captured["conf"]["group.id"] == "my-profiler-group"
    assert captured["conf"]["bootstrap.servers"] == "localhost:9092"
    assert captured["topics"] == ["test-topic"]
    assert captured["closed"] is True


def test_group_id_in_the_properties_file_is_honoured(monkeypatch, tmp_path):
    config_file = tmp_path / "client.properties"
    config_file.write_text("bootstrap.servers=localhost:9092\ngroup.id=from-file\n")

    captured = {}
    monkeypatch.setattr(main, "Consumer", _fake_consumer_class(captured))
    monkeypatch.setattr(main, "MessageProcessor", lambda config: make_processor([]))
    _set_env(monkeypatch, config_file)
    monkeypatch.delenv("GROUP_ID")

    main.main()

    assert captured["conf"]["group.id"] == "from-file"


def test_missing_group_id_is_reported_clearly(monkeypatch, tmp_path, caplog):
    config_file = tmp_path / "client.properties"
    config_file.write_text("bootstrap.servers=localhost:9092\n")

    captured = {}
    monkeypatch.setattr(main, "Consumer", _fake_consumer_class(captured))
    monkeypatch.setattr(main, "MessageProcessor", lambda config: make_processor([]))
    _set_env(monkeypatch, config_file)
    monkeypatch.delenv("GROUP_ID")

    with caplog.at_level("ERROR"):
        main.main()

    assert "No consumer group configured" in caplog.text
    assert "conf" not in captured  # never got as far as building a Consumer


# --------------------------------------------------------------------------
# Timestamp units
# --------------------------------------------------------------------------

def test_formatted_timestamp_is_converted_to_milliseconds():
    """_extract_time1 must return the same unit as _extract_time2 (epoch ms)."""
    config = make_config(t1="value.event_time", date_time_format="%Y-%m-%d %H:%M:%S")
    processor = MessageProcessor(config)
    msg = FakeMessage(json.dumps({"event_time": "2026-09-03 10:00:00"}).encode("utf-8"))

    expected_ms = datetime(2026, 9, 3, 10, 0, 0).timestamp() * 1000

    assert processor._extract_time1(msg) == pytest.approx(expected_ms)


def test_latency_from_a_formatted_timestamp_is_plausible():
    """The seconds/milliseconds mismatch used to report roughly 57 years of latency."""
    config = make_config(t1="value.event_time", date_time_format="%Y-%m-%d %H:%M:%S")
    processor = MessageProcessor(config)
    event_time = datetime.now().replace(microsecond=0)
    msg = FakeMessage(
        json.dumps({"event_time": event_time.strftime("%Y-%m-%d %H:%M:%S")}).encode("utf-8")
    )

    latency = processor.process_message(msg)

    assert latency is not None
    assert 0 <= latency < 60_000, f"implausible latency: {latency} ms"
