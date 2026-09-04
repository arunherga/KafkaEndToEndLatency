"""CSV output: appends, does not overwrite, and works outside a container."""

import csv

from src.core.statistics import RESULT_FIELDS, LatencyReport
from src.output.file_output import output_to_file, write_to_csv


def make_rows(count=1.0, partition='all'):
    report = LatencyReport()
    report.record(count, partition=0)
    return report.rows({'topic': 'orders', 'date_time': '2026-09-04 10:00:00.000000'})


def read_csv(path):
    with open(path, newline='', encoding='utf-8') as fh:
        return list(csv.DictReader(fh))


def test_results_are_appended_not_overwritten(tmp_path):
    """to_csv() used to overwrite; with restart: unless-stopped that destroyed
    the previous window's results on every restart."""
    target = tmp_path / "latency.csv"

    write_to_csv(str(target), make_rows())
    write_to_csv(str(target), make_rows())
    write_to_csv(str(target), make_rows())

    rows = read_csv(target)
    assert len(rows) == 6  # three runs x (one partition + one aggregate)


def test_the_header_is_written_exactly_once(tmp_path):
    target = tmp_path / "latency.csv"

    write_to_csv(str(target), make_rows())
    write_to_csv(str(target), make_rows())

    lines = target.read_text(encoding="utf-8").strip().splitlines()
    assert lines[0] == ",".join(RESULT_FIELDS)
    assert sum(1 for line in lines if line.startswith("date_time,")) == 1


def test_missing_parent_directories_are_created(tmp_path):
    target = tmp_path / "nested" / "deeper" / "latency.csv"

    write_to_csv(str(target), make_rows())

    assert target.exists()


def test_an_empty_existing_file_still_gets_a_header(tmp_path):
    target = tmp_path / "latency.csv"
    target.touch()

    write_to_csv(str(target), make_rows())

    assert read_csv(target)[0]['topic'] == 'orders'


def test_output_path_is_used_as_given(tmp_path, monkeypatch):
    """The path used to be forced under /app/data, breaking every local run."""
    monkeypatch.chdir(tmp_path)

    class Config:
        local_filepath = "results/latency.csv"

    output_to_file(Config(), make_rows())

    assert (tmp_path / "results" / "latency.csv").exists()


def test_every_result_field_reaches_the_file(tmp_path):
    target = tmp_path / "latency.csv"
    write_to_csv(str(target), make_rows())

    row = read_csv(target)[0]
    assert set(row) == set(RESULT_FIELDS)
    assert row['count'] == '1'
    assert row['partition'] == '0'
