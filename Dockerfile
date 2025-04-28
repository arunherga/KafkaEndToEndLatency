FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p /app/data /app/logs

# Copy the entire project
COPY . .

# Set environment variables
ENV PYTHONPATH=/app

# Run the application
CMD ["python", "main.py"]

ENV RUN_INTERVAL=120
ENV T1='IngestionTime'
ENV T2='consumerWallClockTime'
ENV VALUE_DESERIALIZER='StringDeserializer'
ENV KEY_DESERIALIZER='StringDeserializer'
ENV ENABLE_SAMPLING='False'
ENV DATE_TIME_FORMAT='epoch'
ENV PRODUCER_CONFIG_FILE='None'
ENV RESULT_DUMP_LOCAL_FILEPATH='None'



