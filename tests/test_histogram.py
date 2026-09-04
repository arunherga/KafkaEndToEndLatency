"""The in-repo histogram, checked against brute-force percentiles.

Owning the bucketing means owning its correctness, so these tests compare it
against the exact answer computed from the same data.
"""

import math
import random

import pytest

from src.core.histogram import BoundedHistogram

HIGHEST = 3_600_000_000


def exact_percentile(values, percentile):
    """HdrHistogram semantics: the smallest recorded value whose cumulative
    count reaches ceil(p/100 * n)."""
    ordered = sorted(values)
    target = min(len(ordered), max(1, math.ceil(percentile / 100.0 * len(ordered))))
    return ordered[target - 1]


def relative_error(bits):
    return 2.0 / (1 << bits)


# -- bucket mapping ---------------------------------------------------------

def test_small_values_are_recorded_exactly():
    """Below the sub-bucket count the histogram is lossless."""
    histogram = BoundedHistogram(HIGHEST, sub_bucket_bits=11)
    for value in range(0, 2048):
        histogram.record(value)

    for percentile in (0.1, 25, 50, 90, 99, 100):
        assert histogram.value_at_percentile(percentile) == exact_percentile(
            list(range(0, 2048)), percentile
        )


@pytest.mark.parametrize("bits", [4, 7, 11])
@pytest.mark.parametrize(
    "value", [1, 100, 2_047, 2_048, 5_000, 1_000_000, 250_000_000, HIGHEST - 1]
)
def test_reported_value_never_understates_and_stays_within_error(bits, value):
    """A latency must never be reported lower than it was, and never by much."""
    histogram = BoundedHistogram(HIGHEST, sub_bucket_bits=bits)
    histogram.record(value)
    reported = histogram.value_at_percentile(50)

    assert reported >= value
    assert reported <= value * (1 + relative_error(bits)) + 1


@pytest.mark.parametrize("bits", [7, 11])
def test_matches_brute_force_percentiles_on_random_data(bits):
    rng = random.Random(20260904)
    values = [rng.randint(1, 50_000_000) for _ in range(20_000)]

    histogram = BoundedHistogram(HIGHEST, sub_bucket_bits=bits)
    for value in values:
        histogram.record(value)

    for percentile in (50, 90, 95, 99, 99.9):
        reported = histogram.value_at_percentile(percentile)
        expected = exact_percentile(values, percentile)
        assert reported >= expected
        assert reported <= expected * (1 + relative_error(bits)) + 1


def test_percentiles_are_monotonic():
    rng = random.Random(7)
    histogram = BoundedHistogram(HIGHEST)
    for _ in range(5_000):
        histogram.record(rng.randint(1, 10_000_000))

    reported = [histogram.value_at_percentile(p) for p in (50, 90, 95, 99, 99.9, 100)]
    assert reported == sorted(reported)


def test_a_skewed_tail_is_visible_in_the_high_percentiles():
    """The case the tool exists for: most messages fast, a few very slow."""
    histogram = BoundedHistogram(HIGHEST)
    for _ in range(9_900):
        histogram.record(1_000)      # 1 ms
    for _ in range(100):
        histogram.record(5_000_000)  # 5 s

    assert histogram.value_at_percentile(50) == pytest.approx(1_000, rel=0.01)
    assert histogram.value_at_percentile(99.5) >= 5_000_000


# -- bookkeeping ------------------------------------------------------------

def test_empty_histogram_reports_nothing():
    assert BoundedHistogram(HIGHEST).value_at_percentile(99) is None


def test_values_beyond_the_ceiling_are_counted_not_grown_into():
    histogram = BoundedHistogram(1_000_000, sub_bucket_bits=7)
    buckets_before = histogram.bucket_count

    histogram.record(50_000_000_000)

    assert histogram.overflow == 1
    assert histogram.total == 1
    assert histogram.bucket_count == buckets_before


def test_negative_values_are_rejected():
    with pytest.raises(ValueError, match="negative"):
        BoundedHistogram(HIGHEST).record(-1)


def test_zero_is_recordable():
    histogram = BoundedHistogram(HIGHEST)
    histogram.record(0)
    assert histogram.value_at_percentile(50) == 0


@pytest.mark.parametrize("bits, ceiling_kib", [(7, 20), (11, 200)])
def test_memory_is_bounded_regardless_of_message_count(bits, ceiling_kib):
    histogram = BoundedHistogram(HIGHEST, sub_bucket_bits=bits)
    before = histogram.memory_bytes

    rng = random.Random(1)
    for _ in range(100_000):
        histogram.record(rng.randint(1, HIGHEST - 1))

    assert histogram.memory_bytes == before
    assert histogram.memory_bytes < ceiling_kib * 1024


def test_sub_bucket_bits_must_be_positive():
    with pytest.raises(ValueError, match="sub_bucket_bits"):
        BoundedHistogram(HIGHEST, sub_bucket_bits=0)
