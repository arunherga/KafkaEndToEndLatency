"""Latency statistics: exact aggregates, approximate percentiles, per-partition."""

import statistics as stdlib_statistics

import pytest

from src.core.statistics import RESULT_FIELDS, LatencyReport, LatencyStats

# -- exact aggregates -------------------------------------------------------

def test_count_min_max_mean_and_stddev_are_exact():
    """These are running aggregates, not histogram estimates, so they are exact."""
    values = [12.5, 3.25, 99.0, 41.75, 7.5]
    stats = LatencyStats()
    for value in values:
        stats.record(value)

    assert stats.count == 5
    assert stats.minimum == 3.25
    assert stats.maximum == 99.0
    assert stats.mean == pytest.approx(stdlib_statistics.fmean(values))
    assert stats.stddev == pytest.approx(stdlib_statistics.stdev(values))


def test_mean_stays_exact_over_many_values():
    """Welford's algorithm, not a running sum that drifts."""
    stats = LatencyStats()
    for value in range(1, 100_001):
        stats.record(float(value))

    assert stats.mean == pytest.approx(50_000.5, rel=1e-12)


def test_empty_stats_report_none_rather_than_zero():
    stats = LatencyStats()
    assert stats.count == 0
    assert stats.mean is None
    assert stats.stddev is None
    assert stats.minimum is None
    assert stats.percentile(99) is None


def test_a_single_value_has_zero_standard_deviation():
    stats = LatencyStats()
    stats.record(42.0)
    assert stats.stddev == 0.0


# -- percentiles ------------------------------------------------------------

def test_percentiles_track_the_real_distribution():
    stats = LatencyStats()
    for value in range(1, 10_001):
        stats.record(float(value))

    assert stats.percentile(50) == pytest.approx(5_000, rel=0.01)
    assert stats.percentile(99) == pytest.approx(9_900, rel=0.01)
    assert stats.percentile(99.9) == pytest.approx(9_990, rel=0.01)


def test_sub_millisecond_latencies_keep_their_resolution():
    """Recording in microseconds internally means 0.25 ms is not rounded to 0."""
    stats = LatencyStats()
    for _ in range(100):
        stats.record(0.25)

    assert stats.percentile(50) == pytest.approx(0.25, rel=0.01)


def test_memory_does_not_grow_with_message_count():
    stats = LatencyStats()
    before = stats._histogram.memory_bytes
    for value in range(200_000):
        stats.record(float(value % 5_000))

    assert stats._histogram.memory_bytes == before
    assert stats.count == 200_000


# -- per-partition ----------------------------------------------------------

def test_partitions_are_tracked_separately():
    """One slow partition is invisible in a single topic-wide percentile."""
    report = LatencyReport()
    for _ in range(1_000):
        report.record(1.0, partition=0)
    for _ in range(1_000):
        report.record(500.0, partition=1)

    assert report.by_partition[0].percentile(99) == pytest.approx(1.0, rel=0.02)
    assert report.by_partition[1].percentile(99) == pytest.approx(500.0, rel=0.02)
    assert report.overall.count == 2_000
    assert report.overall.maximum == 500.0


def test_recording_without_a_partition_still_counts_overall():
    report = LatencyReport()
    report.record(5.0)

    assert report.count == 1
    assert report.by_partition == {}


# -- result rows ------------------------------------------------------------

def test_rows_are_one_per_partition_plus_an_aggregate():
    report = LatencyReport()
    report.record(1.0, partition=2)
    report.record(2.0, partition=0)
    report.record(3.0, partition=1)

    rows = report.rows({'topic': 'orders'})

    assert [row['partition'] for row in rows] == ['0', '1', '2', 'all']
    assert rows[-1]['count'] == 3
    assert all(row['topic'] == 'orders' for row in rows)


def test_rows_use_the_canonical_field_order():
    """The CSV header and the Avro record both depend on this order."""
    report = LatencyReport()
    report.record(1.0, partition=0)

    for row in report.rows({'topic': 'orders'}):
        assert list(row) == RESULT_FIELDS


def test_context_fields_that_are_absent_come_back_as_none():
    report = LatencyReport()
    report.record(1.0, partition=0)

    rows = report.rows({})
    assert rows[0]['topic'] is None
    assert rows[0]['window_seconds'] is None
