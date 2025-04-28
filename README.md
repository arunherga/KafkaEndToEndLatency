# Kafka Latency Profiler

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
```

2. Run the application:
```bash
python main.py
```

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
