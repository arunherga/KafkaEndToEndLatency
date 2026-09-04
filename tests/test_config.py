"""Configuration validation: fail loudly rather than produce no output."""

import pytest

from src.config.config_manager import create_kafka_config, read_sr_config, validate_config

ENV = {
    "INPUT_TOPIC": "test-topic",
    "GROUP_ID": "test-group",
    "RUN_INTERVAL": "120",
    "T1": "IngestionTime",
    "T2": "consumerWallClockTime",
    "CONSUMER_OUTPUT": "localFileDump",
    "RESULT_DUMP_LOCAL_FILEPATH": "out.csv",
    "VALUE_DESERIALIZER": "StringDeserializer",
    "KEY_DESERIALIZER": "StringDeserializer",
    "DATE_TIME_FORMAT": "epoch",
}


@pytest.fixture
def env(monkeypatch, tmp_path):
    """A complete, valid environment that individual tests can break."""
    config_file = tmp_path / "client.properties"
    config_file.write_text("bootstrap.servers=localhost:9092\n")

    for key in list(ENV) + ["ENABLE_SAMPLING", "T1_UNIT", "T1_TIMEZONE",
                            "PRODUCER_CONFIG_FILE", "OUTPUT_TOPIC"]:
        monkeypatch.delenv(key, raising=False)
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("CONSUMER_CONFIG_FILE", str(config_file))
    return monkeypatch


def test_a_complete_environment_validates(env):
    config = create_kafka_config()
    assert config.input_topic == "test-topic"
    assert config.t1_unit == "ms"
    assert config.t1_timezone == "utc"


# -- settings that used to fail silently ------------------------------------

def test_an_unrecognised_output_type_is_rejected(env):
    """A typo used to run the whole window and then write nothing at all."""
    env.setenv("CONSUMER_OUTPUT", "localfiledump")
    with pytest.raises(ValueError, match="CONSUMER_OUTPUT"):
        create_kafka_config()


def test_a_missing_required_setting_names_itself(env):
    env.delenv("INPUT_TOPIC")
    with pytest.raises(ValueError, match="INPUT_TOPIC is required"):
        create_kafka_config()


def test_run_interval_must_be_positive(env):
    """0 meant 'exit immediately', which surfaced as a confusing empty run."""
    env.setenv("RUN_INTERVAL", "0")
    with pytest.raises(ValueError, match="RUN_INTERVAL must be a positive"):
        create_kafka_config()


def test_an_unusable_t1_spec_is_rejected(env):
    env.setenv("T1", "headers.ts")
    with pytest.raises(ValueError, match="Invalid input for T1"):
        create_kafka_config()


# -- new settings -----------------------------------------------------------

@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_every_epoch_unit_is_accepted(env, unit):
    env.setenv("T1_UNIT", unit)
    assert create_kafka_config().t1_unit == unit


def test_an_unknown_epoch_unit_is_rejected(env):
    env.setenv("T1_UNIT", "furlongs")
    with pytest.raises(ValueError, match="T1_UNIT"):
        create_kafka_config()


def test_units_and_timezones_are_case_insensitive(env):
    env.setenv("T1_UNIT", "MS")
    env.setenv("T1_TIMEZONE", "UTC")
    config = create_kafka_config()
    assert (config.t1_unit, config.t1_timezone) == ("ms", "utc")


def test_an_unknown_timezone_is_rejected(env):
    env.setenv("T1_TIMEZONE", "mars")
    with pytest.raises(ValueError, match="T1_TIMEZONE"):
        create_kafka_config()


# -- removed settings -------------------------------------------------------

def test_enable_sampling_is_ignored_with_an_explanation(env, caplog):
    """It never saved memory and made the mean disagree with the percentiles."""
    env.setenv("ENABLE_SAMPLING", "True")

    with caplog.at_level("WARNING"):
        config = create_kafka_config()

    assert "ENABLE_SAMPLING is no longer supported" in caplog.text
    assert not hasattr(config, "enable_sampling")


def test_no_warning_when_enable_sampling_is_absent(env, caplog):
    with caplog.at_level("WARNING"):
        create_kafka_config()

    assert "ENABLE_SAMPLING" not in caplog.text


# -- schema registry config -------------------------------------------------

def test_schema_registry_auth_is_optional(tmp_path):
    """An unauthenticated local Schema Registry used to raise KeyError."""
    config_file = tmp_path / "client.properties"
    config_file.write_text("schema.registry.url=http://localhost:8081\n")

    assert read_sr_config(str(config_file)) == {'url': 'http://localhost:8081'}


def test_schema_registry_auth_is_passed_through_when_present(tmp_path):
    config_file = tmp_path / "client.properties"
    config_file.write_text(
        "schema.registry.url=http://localhost:8081\nbasic.auth.user.info=user:secret\n"
    )

    assert read_sr_config(str(config_file)) == {
        'url': 'http://localhost:8081',
        'basic.auth.user.info': 'user:secret',
    }


def test_a_missing_schema_registry_url_is_reported(tmp_path):
    config_file = tmp_path / "client.properties"
    config_file.write_text("bootstrap.servers=localhost:9092\n")

    with pytest.raises(ValueError, match="schema.registry.url is required"):
        read_sr_config(str(config_file))


# -- output-specific requirements -------------------------------------------

def test_dump_to_topic_requires_a_producer_config(make_config):
    config = make_config(output_type="dumpToTopic", producer_config_file=None,
                         output_topic="results")
    with pytest.raises(ValueError, match="PRODUCER_CONFIG_FILE"):
        validate_config(config)


def test_dump_to_topic_requires_an_output_topic(make_config):
    config = make_config(output_type="dumpToTopic",
                         producer_config_file="producer.properties", output_topic=None)
    with pytest.raises(ValueError, match="OUTPUT_TOPIC"):
        validate_config(config)
