# Kafka Latency Profiler

[![CI](https://github.com/arunherga/KafkaEndToEndLatency/actions/workflows/ci.yml/badge.svg)](https://github.com/arunherga/KafkaEndToEndLatency/actions/workflows/ci.yml)

A tool for measuring and analyzing message latency in Kafka topics. This profiler can measure latency between different timestamps in Kafka messages and output the results to either a Kafka topic or a local file.

## Features

- Measure latency between different timestamps in Kafka messages
- Support for various deserializers (Avro, JSON Schema, JSON, String)
- Per-partition breakdown as well as topic-wide figures
- Fixed-memory percentiles, so a busy topic cannot exhaust the heap
- Output results to a Kafka topic or a local CSV
- Docker support for easy deployment

## Project Structure

```
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

Two configuration files, deliberately:

| File | Format | Holds |
|---|---|---|
| `client.properties` | java properties | Kafka connection: `bootstrap.servers`, SASL, Schema Registry. Passed straight to librdkafka. **Gitignored** -- this is where credentials go. |
| `arguments.env` | environment variables | Application settings: `INPUT_TOPIC`, `T1`, `T2`, ... Tracked as a template. |

They are not interchangeable. librdkafka rejects `INPUT_TOPIC` as an unknown
property, so pointing `CONSUMER_CONFIG_FILE` at the wrong one fails at startup.

## Prerequisites

- Python 3.9 or higher (CI covers 3.9, 3.11 and 3.13)
- Docker and Docker Compose (for containerized deployment)
- Kafka cluster access
- Schema Registry (if using Avro or JSON Schema)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd kafka-latency-profiler
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

1. Copy the Kafka connection template and fill it in. `client.properties` is
   gitignored, so credentials stay out of the repository:
```bash
cp client.properties.example client.properties
```
```
bootstrap.servers=your-kafka-broker:9092
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=your-username
sasl.password=your-password
group.id=kafka-latency-profiler
auto.offset.reset=latest

# Only for AvroDeserializer / JSONSchemaDeserializer
schema.registry.url=your-schema-registry-url
basic.auth.user.info=your-schema-registry-credentials
```

2. Configure application settings as environment variables, in
   `docker-compose.yml` or your shell:
```yaml
environment:
  - CONSUMER_CONFIG_FILE=/app/client.properties
  - INPUT_TOPIC=your-input-topic
  - GROUP_ID=your-group-id
  - RUN_INTERVAL=120
  - T1=IngestionTime
  - T2=consumerWallClockTime
  - CONSUMER_OUTPUT=localFileDump
  - RESULT_DUMP_LOCAL_FILEPATH=/app/data/latency_results.csv
  - VALUE_DESERIALIZER=StringDeserializer
  - KEY_DESERIALIZER=StringDeserializer
  - DATE_TIME_FORMAT=epoch
  - T1_UNIT=ms
  - T1_TIMEZONE=utc
```

## Running the Application

### Using Docker

1. Build and run the container:
```bash
docker-compose up --build
```

2. Monitor logs:
```bash
docker-compose logs -f
```

3. Stop the application:
```bash
docker-compose down
```

The container handles `SIGTERM`, so stopping it ends the current window and
writes out whatever it measured rather than discarding the run. `docker-compose.yml`
allows 30 seconds for that.

The profiler measures one window and exits, so `restart` is `on-failure`: a
completed run stops the container, and only a genuine crash is retried.

### Running Locally

1. Set up the environment variables:
```bash
export CONSUMER_CONFIG_FILE=client.properties
export INPUT_TOPIC=your-input-topic
export GROUP_ID=your-group-id
export RUN_INTERVAL=120
export T1=IngestionTime
export T2=consumerWallClockTime
export CONSUMER_OUTPUT=localFileDump
export RESULT_DUMP_LOCAL_FILEPATH=latency_results.csv
export VALUE_DESERIALIZER=StringDeserializer
export KEY_DESERIALIZER=StringDeserializer
export DATE_TIME_FORMAT=epoch
export T1_UNIT=ms
export T1_TIMEZONE=utc
```

2. Run the application:
```bash
python main.py
```

## Running the Tests

```bash
pip install -r requirements-dev.txt
pytest
```

The suite runs entirely against fakes -- no broker or Schema Registry needed.

Linting uses [ruff](https://docs.astral.sh/ruff/), configured in `pyproject.toml`:

```bash
pip install ruff==0.8.6
ruff check .
```

Both run on every pull request via GitHub Actions.

## What Is Actually Being Measured

`T1` and `T2` are normally read from **different clocks**, so a latency is only as
trustworthy as the sync between them:

| Setting | Clock |
|---|---|
| `T1=IngestionTime` | The broker's record of the message timestamp |
| `T1=value.<field>` / `T1=key.<field>` | Whatever host wrote that field, usually the producer |
| `T2=consumerWallClockTime` | The consumer host running this tool |
| `T2=IngestionTime` | The broker |

Keep every host NTP-synced. The profiler counts latencies that come out negative
and warns about them at the end of a run, since that is the clearest signal that
two clocks disagree.

Two further caveats:

- `T1=IngestionTime` reads `message.timestamp.type` as configured on the topic.
  That is `CreateTime` (the **producer's** clock) unless the topic sets
  `LogAppendTime` (the **broker's**). The run logs which one it observed.
- `T2=consumerWallClockTime` is captured the moment `poll()` returns, before any
  deserialization, so decoding cost is not counted as latency.

### Timestamp units and timezones

An epoch field carries no unit, so `T1_UNIT` supplies it (`s`, `ms`, `us`, `ns`;
default `ms`). Getting this wrong is a silent 1000x error, not a crash.

When `DATE_TIME_FORMAT` is a strptime pattern rather than `epoch`, the parsed
value usually has no timezone. `T1_TIMEZONE` decides how to read it -- `utc`
(default) or `local`. Without this the same message yields different latencies
depending on the consumer host's `TZ`.

`T1` accepts nested paths (`value.header.produced_at`) and reads from the message
key as well as the value (`key.produced_at`).

## Output

Every run emits **one row per partition plus an `all` aggregate row**, with these
columns:

| Column | Meaning |
|---|---|
| `date_time` | When the run finished (UTC) |
| `topic`, `group_id`, `partition` | What was measured; `partition=all` is the aggregate |
| `count` | Latencies measured |
| `skipped` | Messages read but not measurable (see the run's diagnostics) |
| `negative_latencies` | Measured below zero, i.e. clock skew |
| `window_seconds` | Actual length of the run |
| `min_ms`, `mean_ms`, `max_ms`, `stddev_ms` | Exact, not estimated |
| `p50_ms`, `p90_ms`, `p95_ms`, `p99_ms`, `p999_ms` | From the histogram, within ~0.1% |

A per-partition breakdown is usually the useful diagnostic: one slow broker is
invisible in a single topic-wide p99.

1. **Kafka Topic** (`CONSUMER_OUTPUT=dumpToTopic`) publishes one Avro message
   per row to `OUTPUT_TOPIC`, keyed by topic, group, partition and time.
2. **Local File** (`CONSUMER_OUTPUT=localFileDump`) **appends** the rows to the
   CSV at `RESULT_DUMP_LOCAL_FILEPATH`, writing the header only when creating
   the file. The path is used exactly as given, relative to the working
   directory.

### Memory and accuracy

Percentiles come from a fixed-memory log-linear histogram (~180 KiB overall,
~14 KiB per partition) rather than a list of every latency, so a busy topic over
a long window no longer grows without bound. Percentile values carry a bounded
relative error of about 0.1% overall and 1.6% per partition, and are never
reported lower than the real value. Count, min, max, mean and standard deviation
are tracked exactly.

`ENABLE_SAMPLING` has been removed. It never reduced memory -- it sampled the
list after it had already been built -- and it applied only to the mean, so the
mean and the percentiles described different populations. Setting it now logs a
warning and is otherwise ignored.

## Logging

- Logs go to stdout/stderr, which is where a container's logs belong. (An
  earlier version of this README claimed a `logs/` directory; nothing ever
  wrote to it.)
- Log level is configured in `main.py`
- Docker logs can be viewed with `docker-compose logs -f`

Each run reports how many messages it measured, how many it skipped and why,
which message timestamp type it observed, and how many latencies came out
negative, before it reports any percentiles.

The process exits non-zero if the run failed, so `restart: on-failure` and CI
can tell a failed run from a completed one.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request
