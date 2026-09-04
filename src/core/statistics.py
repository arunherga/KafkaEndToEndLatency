"""Bounded-memory latency statistics.

The previous implementation appended every latency to a list, so a busy topic
over a long window exhausted memory. The "sampling" option that was meant to
help did nothing for memory -- it ran after the fact, over the list that had
already been built, and shrank only the mean while the percentiles still used
every value. The two numbers therefore described different populations.

Percentiles here come from a log-linear histogram (see src/core/histogram.py):
fixed memory regardless of how many messages arrive, with a bounded relative
error. Count, min, max, mean and standard deviation are tracked exactly with
running aggregates, which cost nothing on top of that.
"""

import math

from src.core.histogram import BoundedHistogram

# Latencies are recorded in microseconds so that sub-millisecond values keep
# their resolution, and reported in milliseconds.
MICROS_PER_MILLI = 1000
LOWEST_TRACKABLE_US = 1
HIGHEST_TRACKABLE_US = 3_600_000_000  # one hour

# 11 bits of sub-bucket gives ~0.1% relative error for ~190 KiB. Per-partition
# histograms use 7 bits (~1.6% for ~14 KiB) because there is one of them per
# partition, and a per-partition breakdown is read for its shape rather than
# its third significant digit.
AGGREGATE_SUB_BUCKET_BITS = 11
PARTITION_SUB_BUCKET_BITS = 7

PERCENTILES = (50.0, 90.0, 95.0, 99.0, 99.9)

# Column order for both the CSV and the Avro record.
RESULT_FIELDS = [
    'date_time',
    'topic',
    'group_id',
    'partition',
    'count',
    'skipped',
    'negative_latencies',
    'window_seconds',
    'min_ms',
    'mean_ms',
    'max_ms',
    'stddev_ms',
    'p50_ms',
    'p90_ms',
    'p95_ms',
    'p99_ms',
    'p999_ms',
]

PERCENTILE_FIELDS = {
    50.0: 'p50_ms',
    90.0: 'p90_ms',
    95.0: 'p95_ms',
    99.0: 'p99_ms',
    99.9: 'p999_ms',
}


class LatencyStats:
    """Streaming statistics for one stream of latencies, in milliseconds."""

    def __init__(self, sub_bucket_bits: int = AGGREGATE_SUB_BUCKET_BITS):
        self._histogram = BoundedHistogram(HIGHEST_TRACKABLE_US, sub_bucket_bits)
        self.count = 0
        self.minimum = None
        self.maximum = None
        self._mean = 0.0
        self._m2 = 0.0

    def record(self, latency_ms: float) -> None:
        """Record one non-negative latency in milliseconds."""
        self.count += 1
        if self.minimum is None or latency_ms < self.minimum:
            self.minimum = latency_ms
        if self.maximum is None or latency_ms > self.maximum:
            self.maximum = latency_ms

        # Welford's algorithm: exact mean and variance in a single pass,
        # without keeping the values around.
        delta = latency_ms - self._mean
        self._mean += delta / self.count
        self._m2 += delta * (latency_ms - self._mean)

        self._histogram.record(max(LOWEST_TRACKABLE_US, round(latency_ms * MICROS_PER_MILLI)))

    @property
    def mean(self):
        return self._mean if self.count else None

    @property
    def stddev(self):
        if self.count == 0:
            return None
        if self.count == 1:
            return 0.0
        return math.sqrt(self._m2 / (self.count - 1))

    def percentile(self, percentile: float):
        """The latency in milliseconds at the given percentile."""
        if not self.count:
            return None
        return self._histogram.value_at_percentile(percentile) / MICROS_PER_MILLI

    def as_dict(self) -> dict:
        summary = {
            'count': self.count,
            'min_ms': self.minimum,
            'mean_ms': self.mean,
            'max_ms': self.maximum,
            'stddev_ms': self.stddev,
        }
        for percentile, field in PERCENTILE_FIELDS.items():
            summary[field] = self.percentile(percentile)
        return summary


class LatencyReport:
    """Overall latency statistics plus a per-partition breakdown.

    Which partition is slow is usually the useful diagnostic -- a single p99
    over every partition hides one bad broker in an otherwise healthy topic.
    """

    def __init__(self):
        self.overall = LatencyStats(AGGREGATE_SUB_BUCKET_BITS)
        self.by_partition = {}

    def record(self, latency_ms: float, partition=None) -> None:
        self.overall.record(latency_ms)
        if partition is None:
            return
        stats = self.by_partition.get(partition)
        if stats is None:
            stats = self.by_partition[partition] = LatencyStats(PARTITION_SUB_BUCKET_BITS)
        stats.record(latency_ms)

    @property
    def count(self) -> int:
        return self.overall.count

    def rows(self, context: dict) -> list:
        """One row per partition, then an 'all' row, each carrying `context`."""
        rows = []
        for partition in sorted(self.by_partition):
            rows.append({**context, 'partition': str(partition),
                         **self.by_partition[partition].as_dict()})
        rows.append({**context, 'partition': 'all', **self.overall.as_dict()})
        return [{field: row.get(field) for field in RESULT_FIELDS} for row in rows]
