"""SIGTERM handling: `docker stop` must not discard the window's measurements."""

import signal

import pytest

import main

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
def restore_signal_handlers():
    """Leave the interpreter's handlers as we found them."""
    saved = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    yield
    for sig, handler in saved.items():
        signal.signal(sig, handler)


def test_a_fresh_request_is_not_set():
    assert main.ShutdownRequest().requested is False


def test_requesting_shutdown_sets_the_flag_and_says_which_signal(caplog):
    shutdown = main.ShutdownRequest()

    with caplog.at_level("INFO"):
        shutdown.request(signal.SIGTERM)

    assert shutdown.requested is True
    assert "SIGTERM" in caplog.text
    assert "reporting what was measured" in caplog.text


def test_handlers_are_installed_for_both_signals(restore_signal_handlers):
    shutdown = main.ShutdownRequest()
    main.install_signal_handlers(shutdown)

    assert signal.getsignal(signal.SIGTERM) == shutdown.request
    assert signal.getsignal(signal.SIGINT) == shutdown.request


def test_installing_handlers_off_the_main_thread_is_not_fatal(monkeypatch):
    """A ValueError from signal.signal must not take the run down with it."""
    def refuse(*_args):
        raise ValueError("signal only works in main thread")

    monkeypatch.setattr(signal, "signal", refuse)
    main.install_signal_handlers(main.ShutdownRequest())  # must not raise


def test_a_shutdown_request_ends_the_window_and_still_reports(
    monkeypatch, tmp_path, make_processor, restore_signal_handlers
):
    """The whole point: results survive the stop signal."""
    config_file = tmp_path / "client.properties"
    config_file.write_text("bootstrap.servers=localhost:9092\n")

    captured = {}
    monkeypatch.setattr(main, "install_signal_handlers",
                        lambda shutdown: captured.__setitem__('shutdown', shutdown))

    polls = {'count': 0}

    class FakeConsumer:
        def __init__(self, conf):
            pass

        def subscribe(self, topics):
            pass

        def poll(self, timeout):
            polls['count'] += 1
            if polls['count'] == 3:
                captured['shutdown'].request(signal.SIGTERM)
            return None

        def close(self):
            captured['closed'] = True

    processor = make_processor([10.0, 20.0, 30.0])
    monkeypatch.setattr(main, "Consumer", FakeConsumer)
    monkeypatch.setattr(main, "MessageProcessor", lambda config: processor)

    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("CONSUMER_CONFIG_FILE", str(config_file))
    monkeypatch.setenv("RESULT_DUMP_LOCAL_FILEPATH", str(tmp_path / "out.csv"))

    main.main()

    # RUN_INTERVAL is 120s; the loop stopped on the request, not the clock.
    assert polls['count'] == 3
    assert captured['closed'] is True
    assert processor.diagnostics_logged is True
    assert (tmp_path / "out.csv").exists()


def test_a_configuration_failure_exits_non_zero(monkeypatch, caplog):
    """`restart: on-failure` and CI both need a failed run to look failed."""
    for key in ENV:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("CONSUMER_CONFIG_FILE", raising=False)

    with caplog.at_level("ERROR"):
        assert main.main() == 1

    assert "is required but was not set" in caplog.text


def test_a_completed_run_exits_zero(monkeypatch, tmp_path, make_processor,
                                    restore_signal_handlers):
    config_file = tmp_path / "client.properties"
    config_file.write_text("bootstrap.servers=localhost:9092\n")

    class FakeConsumer:
        def __init__(self, conf):
            pass

        def subscribe(self, topics):
            pass

        def poll(self, timeout):
            raise KeyboardInterrupt

        def close(self):
            pass

    monkeypatch.setattr(main, "Consumer", FakeConsumer)
    monkeypatch.setattr(main, "MessageProcessor", lambda config: make_processor([]))

    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("CONSUMER_CONFIG_FILE", str(config_file))

    assert main.main() == 0
