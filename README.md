# Kafka Latency Profiler

[![CI](https://github.com/arunherga/KafkaEndToEndLatency/actions/workflows/ci.yml/badge.svg)](https://github.com/arunherga/KafkaEndToEndLatency/actions/workflows/ci.yml)

A tool for measuring and analyzing message latency in Kafka topics. This profiler can measure latency between different timestamps in Kafka messages and output the results to either a Kafka topic or a local file.

## Features

- Measure latency between different timestamps in Kafka messages
- Support for various deserializers (Avro, JSON, String)
- Configurable sampling
- Output results to Kafka topic or local file
- Docker support for easy deployment

## Project Structure

```
kafka-latency-profiler/
├── src/
│   ├── config/         # Configuration management
│   ├── core/          # Core message processing logic
│   └── output/        # Output handlers (Kafka and file)
├── data/              # Output data directory
├── logs/              # Log files directory
├── main.py           # Application entry point
├── requirements.txt   # Python dependencies
├── Dockerfile        # Docker configuration
├── docker-compose.yml # Docker Compose configuration
└── arguments.env     # Kafka configuration
```

## Prerequisites

- Python 3.9 or higher
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

1. Configure Kafka settings in `arguments.env`:
```
bootstrap.servers=your-kafka-broker:9092
security.protocol=PLAINTEXT
sasl.mechanisms=PLAIN
sasl.username=your-username
sasl.password=your-password
schema.registry.url=your-schema-registry-url
basic.auth.user.info=your-schema-registry-credentials
```

2. Configure environment variables in `docker-compose.yml` or set them in your environment:
```yaml
environment:
  - CONSUMER_CONFIG_FILE=/app/arguments.env
  - PRODUCER_CONFIG_FILE=/app/arguments.env
  - INPUT_TOPIC=your-input-topic
  - GROUP_ID=your-group-id
  - ENABLE_SAMPLING=True
  - RUN_INTERVAL=120
  - T1=IngestionTime
  - T2=consumerWallClockTime
  - CONSUMER_OUTPUT=localFileDump
  - RESULT_DUMP_LOCAL_FILEPATH=latency_results.csv
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

### Running Locally

1. Set up the environment variables:
```bash
export CONSUMER_CONFIG_FILE=arguments.env
export PRODUCER_CONFIG_FILE=arguments.env
export INPUT_TOPIC=your-input-topic
export GROUP_ID=your-group-id
export ENABLE_SAMPLING=True
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

The profiler can output results in two ways:

1. **Kafka Topic** (`CONSUMER_OUTPUT=dumpToTopic`):
   - Results are published to the specified output topic
   - Uses Avro serialization
   - Includes average latency and percentiles

2. **Local File** (`CONSUMER_OUTPUT=localFileDump`):
   - Results are written to a CSV file
   - File is stored in the `data` directory
   - Includes average latency and percentiles

## Logging

- Logs are written to the `logs` directory
- Log level can be configured in `main.py`
- Docker logs can be viewed using `docker-compose logs -f`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request
