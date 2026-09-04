"""The single place where a timestamp becomes epoch milliseconds.

A latency is only meaningful if both ends of the subtraction share a unit and
an epoch. Previously they did not: the strptime path produced seconds while the
consumer clock produced milliseconds, and an epoch field was passed through
with whatever unit the producer happened to use. Every conversion now goes
through this module so there is exactly one place to get it right.
"""

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

# Multiplier that takes a value in the named unit to milliseconds.
EPOCH_UNITS = {
    "s": 1000.0,
    "ms": 1.0,
    "us": 0.001,
    "ns": 0.000001,
}

TIMEZONES = ("utc", "local")


class TimestampError(ValueError):
    """A timestamp could not be normalised to epoch milliseconds."""


def epoch_to_millis(raw: Any, unit: str) -> float:
    """Scale a numeric epoch timestamp expressed in `unit` to milliseconds."""
    try:
        multiplier = EPOCH_UNITS[unit]
    except KeyError:
        raise TimestampError(
            f"Unknown epoch unit {unit!r}; expected one of {', '.join(sorted(EPOCH_UNITS))}"
        ) from None

    if isinstance(raw, bool) or not isinstance(raw, (int, float, str)):
        raise TimestampError(f"Value {raw!r} is not a numeric epoch timestamp")

    try:
        return float(raw) * multiplier
    except (TypeError, ValueError):
        raise TimestampError(f"Value {raw!r} is not a numeric epoch timestamp") from None


def formatted_to_millis(raw: Any, date_time_format: str, tz: str = "utc") -> float:
    """Parse a formatted timestamp string to epoch milliseconds.

    strptime returns a naive datetime, and datetime.timestamp() silently reads a
    naive value as the host's local time -- so the same message yields different
    latencies depending on the consumer's TZ. The zone is therefore explicit.
    """
    if tz not in TIMEZONES:
        raise TimestampError(f"Unknown timezone {tz!r}; expected one of {', '.join(TIMEZONES)}")

    if not isinstance(raw, str):
        raise TimestampError(f"Value {raw!r} is not a timestamp string")

    try:
        parsed = datetime.strptime(raw, date_time_format)
    except ValueError as e:
        raise TimestampError(f"Value {raw!r} does not match format {date_time_format!r}: {e}") from e

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc) if tz == "utc" else parsed.astimezone()

    return parsed.timestamp() * 1000


def to_millis(raw: Any, date_time_format: str, unit: str, tz: str = "utc") -> float:
    """Normalise one timestamp field to epoch milliseconds."""
    if date_time_format == "epoch":
        return epoch_to_millis(raw, unit)
    return formatted_to_millis(raw, date_time_format, tz)


def extract_field(payload: Any, path: list) -> Any:
    """Walk a dotted field path (e.g. value.header.event_time) into a payload."""
    current = payload
    for depth, segment in enumerate(path):
        if not isinstance(current, Mapping):
            walked = ".".join(path[:depth]) or "<root>"
            raise TimestampError(f"Cannot read {segment!r}: {walked} is {type(current).__name__}, not a mapping")
        if segment not in current:
            available = ", ".join(sorted(str(k) for k in current)) or "<none>"
            raise TimestampError(f"Field {segment!r} not present; available fields: {available}")
        current = current[segment]
    return current


def split_timestamp_spec(spec: str) -> tuple:
    """Split a T1 value into its source and dotted field path.

    "IngestionTime"           -> ("ingestion", [])
    "value.event_time"        -> ("value", ["event_time"])
    "key.header.produced_at"  -> ("key", ["header", "produced_at"])
    """
    if spec == "IngestionTime":
        return "ingestion", []

    source, _, remainder = spec.partition(".")
    if source not in ("key", "value") or not remainder:
        raise TimestampError(
            f"Invalid timestamp spec {spec!r}; expected IngestionTime, value.<field> or key.<field>"
        )
    path = remainder.split(".")
    if any(not segment for segment in path):
        raise TimestampError(f"Invalid timestamp spec {spec!r}; empty path segment")
    return source, path
