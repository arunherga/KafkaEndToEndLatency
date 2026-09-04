"""A fixed-memory histogram with bounded relative error.

Values are bucketed log-linearly: within each power-of-two magnitude there are
`2 ** sub_bucket_bits` evenly spaced buckets, so a bucket's width scales with
the values it holds and the relative error stays under `2 / sub_bucket_count`
at every magnitude. That is the same shape as HdrHistogram.

It is implemented here rather than taken from the `hdrhistogram` package
because that package ships C extensions with no pure-Python wheel and no Linux
wheel for CPython 3.9 -- the floor this project supports -- so depending on it
would mean building from source in CI and for anyone on an older interpreter.
The algorithm is small enough to own, and `tests/test_histogram.py` checks it
against brute-force percentiles over the exact same data.
"""

import math
from array import array

# Percentiles are reported as the highest value in the bucket that contains
# them, which is what HdrHistogram does: never understate a latency.


class BoundedHistogram:
    def __init__(self, highest_value: int, sub_bucket_bits: int = 11):
        if sub_bucket_bits < 1:
            raise ValueError("sub_bucket_bits must be at least 1")

        self.sub_bucket_bits = sub_bucket_bits
        self.sub_bucket_count = 1 << sub_bucket_bits
        self.highest_value = highest_value

        self._max_index = self._index_for(highest_value)
        # 'Q' is an unsigned 64-bit counter: 8 bytes per bucket, no per-element
        # Python object, so the whole thing is a couple of hundred KiB at most.
        self._counts = array('Q', [0]) * (self._max_index + 1)

        self.total = 0
        self.overflow = 0

    # -- bucket mapping ----------------------------------------------------

    def _index_for(self, value: int) -> int:
        """Map a non-negative value onto its bucket index."""
        if value < self.sub_bucket_count:
            # Linear region: every value below the sub-bucket count is exact.
            return value

        shift = value.bit_length() - self.sub_bucket_bits
        sub = value >> shift
        half = self.sub_bucket_count >> 1
        return self.sub_bucket_count + (shift - 1) * half + (sub - half)

    def _highest_equivalent_value(self, index: int) -> int:
        """The largest value that falls in this bucket."""
        if index < self.sub_bucket_count:
            return index

        half = self.sub_bucket_count >> 1
        remainder = index - self.sub_bucket_count
        shift = remainder // half + 1
        sub = remainder % half + half
        return ((sub + 1) << shift) - 1

    # -- recording ---------------------------------------------------------

    def record(self, value: int) -> None:
        if value < 0:
            raise ValueError(f"cannot record a negative value: {value}")

        index = self._index_for(value)
        if index > self._max_index:
            # Beyond the configured ceiling. Counted so the caller can say the
            # ceiling was hit, and folded into the top bucket rather than
            # growing the array.
            self.overflow += 1
            index = self._max_index

        self._counts[index] += 1
        self.total += 1

    # -- reading -----------------------------------------------------------

    def value_at_percentile(self, percentile: float):
        """The value at the given percentile, or None if nothing was recorded."""
        if self.total == 0:
            return None

        target = min(self.total, max(1, math.ceil(percentile / 100.0 * self.total)))
        running = 0
        for index, count in enumerate(self._counts):
            if not count:
                continue
            running += count
            if running >= target:
                return self._highest_equivalent_value(index)
        return self._highest_equivalent_value(self._max_index)

    @property
    def bucket_count(self) -> int:
        return len(self._counts)

    @property
    def memory_bytes(self) -> int:
        return self._counts.itemsize * len(self._counts)
