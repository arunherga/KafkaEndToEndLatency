# Kafka Latency Profiler

[![CI](https://github.com/arunherga/KafkaEndToEndLatency/actions/workflows/ci.yml/badge.svg)](https://github.com/arunherga/KafkaEndToEndLatency/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.9%20%7C%203.11%20%7C%203.13-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Measures end-to-end message latency on a Kafka topic. The profiler consumes for a
fixed window, computes the difference between two timestamps on every message, and
reports the distribution — overall and per partition — to a Kafka topic or a local
CSV.

## Contents

- [Features](#features)
- [How It Works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Output](#output)
- [Logging and Diagnostics](#logging-and-diagnostics)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Features

- Measures latency between any two timestamps on a message
- Reads timestamps from the broker, the message value, or the message key,
  including nested fields
- Supports Avro, JSON Schema, JSON and String deserializers
- Reports a per-partition breakdown alongside topic-wide figures
- Uses fixed-memory percentiles, so a busy topic cannot exhaust the heap
- Writes results to a Kafka topic or a local CSV
- Ships as a Docker image

## How It Works

The profiler subscribes to `INPUT_TOPIC`, polls for `RUN_INTERVAL` seconds, and for
each message computes `T2 - T1` in milliseconds.

### Clocks and Skew

`T1` and `T2` are normally read from **different clocks**, so a latency is only as
trustworthy as the synchronisation between them.

| Setting | Clock |
|---|---|
| `T1=IngestionTime` | The broker's record of the message timestamp |
| `T1=value.<field>` or `T1=key.<field>` | Whichever host wrote that field, usually the producer |
| `T2=IngestionTime` | The broker |
| `T2=consumerWallClockTime` | The consumer host running this tool |

Keep every host NTP-synced. The profiler counts latencies that come out negative and
warns about them at the end of a run, which is the clearest signal that two clocks
disagree.

Two further caveats:

- `T1=IngestionTime` reads `message.timestamp.type` as configured on the topic. That
  is `CreateTime` (the **producer's** clock) unless the topic sets `LogAppendTime`
  (the **broker's**). Each run logs which one it observed.
- `T2=consumerWallClockTime` is captured the moment `poll()` returns, before any
  deserialization, so decoding cost is not counted as latency.

### Timestamp Units and Timezones

An epoch field carries no unit of its own, so `T1_UNIT` supplies it. Getting this
wrong is a silent 1000x error rather than a crash.

When `DATE_TIME_FORMAT` is a `strptime` pattern rather than `epoch`, the parsed value
usually carries no timezone. `T1_TIMEZONE` decides how to read it. Without this, the
same message yields different latencies depending on the consumer host's `TZ`.

## Prerequisites

- Python 3.9 or higher (CI covers 3.9, 3.11 and 3.13)
- Docker and Docker Compose, for containerised deployment
- Access to a Kafka cluster
- A Schema Registry, if using the Avro or JSON Schema deserializers

## Installation

```bash
git clone https://github.com/arunherga/KafkaEndToEndLatency.git
cd KafkaEndToEndLatency
pip install -r requirements.txt
```

## Configuration

### Configuration Files

Configuration is split across two files, deliberately. They are **not**
interchangeable: librdkafka rejects `INPUT_TOPIC` as an unknown property, so pointing
`CONSUMER_CONFIG_FILE` at the wrong one fails at startup.

| File | Format | Holds |
|---|---|---|
| `client.properties` | java properties | Kafka connection: `bootstrap.servers`, SASL, Schema Registry. Passed straight to librdkafka. **Gitignored** — credentials go here. |
| `arguments.env` | environment variables | Application settings: `INPUT_TOPIC`, `T1`, `T2` and the rest. Tracked as a template. |

### Kafka Connection

Copy the template and fill it in:

```bash
cp client.properties.example client.properties
```

```properties
bootstrap.servers=your-kafka-broker:9092
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=your-username
sasl.password=your-password
group.id=kafka-latency-profiler
auto.offset.reset=latest

# Only required for the Avro or JSON Schema deserializers
schema.registry.url=your-schema-registry-url
basic.auth.user.info=your-schema-registry-credentials
```

Any librdkafka property is valid here.

### Application Settings

All application settings are environment variables.

| Variable | Required | Default | Description |
|---|---|---|---|
| `CONSUMER_CONFIG_FILE` | Yes | — | Path to the Kafka client properties file |
| `INPUT_TOPIC` | Yes | — | Topic to measure |
| `GROUP_ID` | No | — | Consumer group. Overrides `group.id` in the properties file; one of the two must be set |
| `RUN_INTERVAL` | Yes | `120` † | Length of the measurement window, in seconds. Must be positive |
| `T1` | Yes | `IngestionTime` † | Start timestamp: `IngestionTime`, `value.<field>` or `key.<field>`. Nested paths such as `value.header.produced_at` are supported |
| `T2` | Yes | `consumerWallClockTime` † | End timestamp: `IngestionTime` or `consumerWallClockTime` |
| `DATE_TIME_FORMAT` | Yes | `epoch` † | `epoch`, or a `strptime` pattern such as `%Y-%m-%d %H:%M:%S` |
| `T1_UNIT` | No | `ms` | Unit of a numeric `T1` field: `s`, `ms`, `us` or `ns`. Applies when `DATE_TIME_FORMAT=epoch` |
| `T1_TIMEZONE` | No | `utc` | How to read a parsed timestamp with no timezone: `utc` or `local` |
| `VALUE_DESERIALIZER` | Yes | `StringDeserializer` † | `AvroDeserializer`, `JSONSchemaDeserializer`, `JSONDeserializer` or `StringDeserializer` |
| `KEY_DESERIALIZER` | Yes | `StringDeserializer` † | Same values. Only used when `T1` reads from the key |
| `CONSUMER_OUTPUT` | Yes | — | `localFileDump` or `dumpToTopic` |
| `RESULT_DUMP_LOCAL_FILEPATH` | If `localFileDump` | — | CSV path, used exactly as given, relative to the working directory |
| `OUTPUT_TOPIC` | If `dumpToTopic` | — | Topic to publish results to |
| `PRODUCER_CONFIG_FILE` | If `dumpToTopic` | — | Kafka client properties for the producer |

† Supplied by the Docker image. Outside Docker these must be set explicitly.

`ENABLE_SAMPLING` is no longer supported. Setting it logs a warning and is otherwise
ignored; see [Memory and Accuracy](#memory-and-accuracy).

## Usage

### Docker

```bash
docker-compose up --build     # build and run
docker-compose logs -f        # follow the logs
docker-compose down           # stop
```

The container handles `SIGTERM`, so stopping it ends the current window and writes
out whatever it measured rather than discarding the run. `docker-compose.yml` allows
30 seconds for this.

The profiler measures one window and exits, so `restart` is `on-failure`: a completed
run stops the container, and only a genuine crash is retried.

### Local

```bash
export CONSUMER_CONFIG_FILE=client.properties
export INPUT_TOPIC=your-input-topic
export GROUP_ID=your-group-id
export RUN_INTERVAL=120
export T1=IngestionTime
export T2=consumerWallClockTime
export DATE_TIME_FORMAT=epoch
export VALUE_DESERIALIZER=StringDeserializer
export KEY_DESERIALIZER=StringDeserializer
export CONSUMER_OUTPUT=localFileDump
export RESULT_DUMP_LOCAL_FILEPATH=latency_results.csv

python main.py
```

The process exits non-zero if the run fails, so `restart: on-failure` and CI can tell
a failed run from a completed one.

## Output

Every run emits **one row per partition, plus an `all` aggregate row**.

### Result Columns

| Column | Meaning |
|---|---|
| `date_time` | When the run finished (UTC) |
| `topic`, `group_id`, `partition` | What was measured; `partition=all` is the aggregate |
| `count` | Latencies measured |
| `skipped` | Messages read but not measurable; see the run's diagnostics |
| `negative_latencies` | Latencies below zero, indicating clock skew |
| `window_seconds` | Actual length of the run |
| `min_ms`, `mean_ms`, `max_ms`, `stddev_ms` | Exact, not estimated |
| `p50_ms`, `p90_ms`, `p95_ms`, `p99_ms`, `p999_ms` | From the histogram, within ~0.1% |

A per-partition breakdown is usually the useful diagnostic: one slow broker is
invisible in a single topic-wide p99.

### Destinations

| `CONSUMER_OUTPUT` | Behaviour |
|---|---|
| `localFileDump` | **Appends** the rows to the CSV at `RESULT_DUMP_LOCAL_FILEPATH`, writing the header only when creating the file |
| `dumpToTopic` | Publishes one Avro message per row to `OUTPUT_TOPIC`, keyed by topic, group, partition and time |

### Memory and Accuracy

Percentiles come from a fixed-memory log-linear histogram — roughly 180 KiB overall
and 14 KiB per partition — rather than a list of every latency, so a long window on a
busy topic does not grow without bound. Percentile values carry a bounded relative
error of about 0.1% overall and 1.6% per partition, and are never reported lower than
the real value. Count, min, max, mean and standard deviation are tracked exactly.

## Logging and Diagnostics

Logs go to stdout and stderr. The log level is configured in `main.py`, and Docker
logs can be followed with `docker-compose logs -f`.

Before reporting any percentiles, each run reports how much of the data it could
actually use:

```text
Messages seen: 4820 (measured: 4102, skipped: 718)
  skipped 718 message(s): unparsable_t1
  message timestamp type: CreateTime (producer clock) x4820
612 of 4102 latencies were negative (14.9%). T1 and T2 are read from
different hosts, so this normally means their clocks disagree; the
reported percentiles are only as good as that clock sync.
```

Each distinct skip reason is logged once and then counted, so a malformed topic
cannot flood the log.

## Development

### Project Structure

```text
KafkaEndToEndLatency/
├── src/
│   ├── config/               # Configuration management
│   ├── core/                 # Message processing, timestamps, histogram, stats
│   └── output/               # Output handlers (Kafka and file)
├── tests/                    # Test suite; needs no broker
├── main.py                   # Application entry point
├── requirements.txt          # Runtime dependency
├── requirements-dev.txt      # Test and lint tooling
├── pyproject.toml            # ruff and pytest configuration
├── Dockerfile                # Docker configuration
├── docker-compose.yml        # Docker Compose configuration
├── client.properties.example # Kafka connection settings (template)
└── arguments.env             # Application settings (template)
```

### Tests and Linting

```bash
pip install -r requirements-dev.txt
pytest
ruff check .
```

The suite runs entirely against fakes; no broker or Schema Registry is needed. Both
commands run on every pull request via GitHub Actions, along with a Docker image
build.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes, with tests
4. Ensure `pytest` and `ruff check .` both pass
5. Open a pull request

## License

Released under the [MIT License](LICENSE).
