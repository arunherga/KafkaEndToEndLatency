"""Messages in, CSV out, through the real processor and reporting path."""

import csv
import json
import time

import main
from src.core.message_processor import MessageProcessor


def read_csv(path):
    with open(path, newline='', encoding='utf-8') as fh:
        return list(csv.DictReader(fh))


def test_a_run_produces_per_partition_and_aggregate_rows(tmp_path, make_config, make_message):
    target = tmp_path / "latency.csv"
    config = make_config(t1="value.produced_at", local_filepath=str(target))
    processor = MessageProcessor(config)

    now_ms = time.time() * 1000
    # Partition 0 is fast, partition 1 is 500 ms behind.
    for _ in range(200):
        processor.process_message(
            make_message(value=json.dumps({"produced_at": now_ms - 10}).encode(), partition=0)
        )
    for _ in range(200):
        processor.process_message(
            make_message(value=json.dumps({"produced_at": now_ms - 500}).encode(), partition=1)
        )

    main.process_results(processor, config, window_seconds=12.5)

    rows = {row['partition']: row for row in read_csv(target)}
    assert set(rows) == {'0', '1', 'all'}

    assert int(rows['all']['count']) == 400
    assert rows['all']['topic'] == 'test-topic'
    assert rows['all']['group_id'] == 'test-group'
    assert float(rows['all']['window_seconds']) == 12.5

    # The slow partition is visible on its own row, not buried in the aggregate.
    assert float(rows['0']['p99_ms']) < float(rows['1']['p99_ms'])
    assert float(rows['1']['p50_ms']) > 400


def test_unmeasurable_messages_are_counted_in_the_output(tmp_path, make_config, make_message):
    target = tmp_path / "latency.csv"
    config = make_config(t1="value.produced_at", local_filepath=str(target))
    processor = MessageProcessor(config)

    now_ms = time.time() * 1000
    for _ in range(10):
        processor.process_message(
            make_message(value=json.dumps({"produced_at": now_ms}).encode())
        )
    for _ in range(3):
        processor.process_message(make_message(value=json.dumps({"nope": 1}).encode()))

    main.process_results(processor, config, window_seconds=5.0)

    aggregate = [row for row in read_csv(target) if row['partition'] == 'all'][0]
    assert int(aggregate['count']) == 10
    assert int(aggregate['skipped']) == 3


def test_consecutive_runs_append_to_the_same_file(tmp_path, make_config, make_message):
    target = tmp_path / "latency.csv"
    config = make_config(t1="value.produced_at", local_filepath=str(target))
    now_ms = time.time() * 1000

    for _ in range(3):
        processor = MessageProcessor(config)
        for _ in range(5):
            processor.process_message(
                make_message(value=json.dumps({"produced_at": now_ms}).encode())
            )
        main.process_results(processor, config, window_seconds=1.0)

    # Three runs x (one partition + one aggregate), and one header line.
    assert len(read_csv(target)) == 6
    assert target.read_text(encoding="utf-8").count("date_time,topic") == 1


def test_an_empty_run_writes_nothing_and_says_why(tmp_path, make_config, caplog):
    target = tmp_path / "latency.csv"
    config = make_config(local_filepath=str(target))
    processor = MessageProcessor(config)

    with caplog.at_level("WARNING"):
        main.process_results(processor, config, window_seconds=120.0)

    assert not target.exists()
    assert "no latency results" in caplog.text


def test_a_run_of_only_negative_latencies_reports_clock_skew(
    tmp_path, make_config, make_message, caplog
):
    """Every latency below zero means the two hosts' clocks disagree."""
    target = tmp_path / "latency.csv"
    config = make_config(t1="value.produced_at", local_filepath=str(target))
    processor = MessageProcessor(config)

    future_ms = (time.time() + 3600) * 1000
    for _ in range(5):
        processor.process_message(
            make_message(value=json.dumps({"produced_at": future_ms}).encode())
        )

    with caplog.at_level("WARNING"):
        main.process_results(processor, config, window_seconds=1.0)

    assert processor.negative_latencies == 5
    assert not target.exists()
    assert "check their clocks" in caplog.text
