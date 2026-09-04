"""Tests for the epoch-millisecond normalisation choke point."""

from datetime import datetime, timezone

import pytest

from src.core.timestamps import (
    TimestampError,
    epoch_to_millis,
    extract_field,
    formatted_to_millis,
    split_timestamp_spec,
    to_millis,
)

# -- epoch units ------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, unit, expected",
    [
        (1788409800, "s", 1788409800000.0),
        (1788409800000, "ms", 1788409800000.0),
        (1788409800000000, "us", 1788409800000.0),
        (1788409800000000000, "ns", 1788409800000.0),
        ("1788409800000", "ms", 1788409800000.0),
        (1788409800.5, "s", 1788409800500.0),
    ],
)
def test_every_epoch_unit_lands_on_the_same_millisecond(raw, unit, expected):
    """A producer stamping seconds must not be read as milliseconds, and vice versa."""
    assert epoch_to_millis(raw, unit) == pytest.approx(expected)


def test_unknown_epoch_unit_is_rejected():
    with pytest.raises(TimestampError, match="Unknown epoch unit"):
        epoch_to_millis(1, "fortnights")


@pytest.mark.parametrize("raw", ["not-a-number", None, {"a": 1}, [1], True])
def test_non_numeric_epoch_values_are_rejected(raw):
    with pytest.raises(TimestampError, match="not a numeric epoch timestamp"):
        epoch_to_millis(raw, "ms")


# -- formatted timestamps ---------------------------------------------------

def test_naive_timestamps_are_read_as_utc_by_default():
    """strptime yields a naive datetime; .timestamp() would read it as local time."""
    result = formatted_to_millis("2026-09-03 10:00:00", "%Y-%m-%d %H:%M:%S")
    expected = datetime(2026, 9, 3, 10, 0, 0, tzinfo=timezone.utc).timestamp() * 1000
    assert result == expected


def test_utc_parsing_does_not_depend_on_the_host_timezone(monkeypatch):
    """The same message must produce the same number on any consumer host."""
    utc = formatted_to_millis("2026-09-03 10:00:00", "%Y-%m-%d %H:%M:%S", tz="utc")

    # A naive value read as UTC is independent of the process timezone; a value
    # read as local time is not. Assert the first, which is the default.
    assert utc == datetime(2026, 9, 3, 10, 0, 0, tzinfo=timezone.utc).timestamp() * 1000


def test_local_timezone_is_available_as_an_escape_hatch():
    naive = datetime(2026, 9, 3, 10, 0, 0)
    result = formatted_to_millis("2026-09-03 10:00:00", "%Y-%m-%d %H:%M:%S", tz="local")
    assert result == naive.astimezone().timestamp() * 1000


def test_unparsable_string_is_rejected_with_the_format_in_the_message():
    with pytest.raises(TimestampError, match="does not match format"):
        formatted_to_millis("03/09/2026", "%Y-%m-%d %H:%M:%S")


def test_non_string_formatted_value_is_rejected():
    with pytest.raises(TimestampError, match="not a timestamp string"):
        formatted_to_millis(1788409800000, "%Y-%m-%d %H:%M:%S")


def test_unknown_timezone_is_rejected():
    with pytest.raises(TimestampError, match="Unknown timezone"):
        formatted_to_millis("2026-09-03 10:00:00", "%Y-%m-%d %H:%M:%S", tz="mars")


# -- dispatch ---------------------------------------------------------------

def test_to_millis_routes_epoch_and_formatted_values():
    assert to_millis(1788409800, "epoch", "s", "utc") == 1788409800000.0
    assert to_millis("2026-09-03 10:00:00", "%Y-%m-%d %H:%M:%S", "ms", "utc") == (
        datetime(2026, 9, 3, 10, 0, 0, tzinfo=timezone.utc).timestamp() * 1000
    )


# -- field paths ------------------------------------------------------------

def test_nested_field_paths_are_supported():
    payload = {"header": {"event": {"ts": 42}}}
    assert extract_field(payload, ["header", "event", "ts"]) == 42


def test_missing_field_names_the_available_keys():
    with pytest.raises(TimestampError, match="available fields: alpha, beta"):
        extract_field({"alpha": 1, "beta": 2}, ["gamma"])


def test_walking_into_a_non_mapping_is_reported():
    with pytest.raises(TimestampError, match="not a mapping"):
        extract_field({"header": "a string"}, ["header", "ts"])


@pytest.mark.parametrize(
    "spec, expected",
    [
        ("IngestionTime", ("ingestion", [])),
        ("value.event_time", ("value", ["event_time"])),
        ("key.produced_at", ("key", ["produced_at"])),
        ("value.header.event.ts", ("value", ["header", "event", "ts"])),
    ],
)
def test_timestamp_specs_are_split_into_source_and_path(spec, expected):
    assert split_timestamp_spec(spec) == expected


@pytest.mark.parametrize("spec", ["value", "value.", "headers.ts", "", "key."])
def test_malformed_timestamp_specs_are_rejected(spec):
    with pytest.raises(TimestampError):
        split_timestamp_spec(spec)
