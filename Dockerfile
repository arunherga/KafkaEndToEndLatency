# Bookworm is pinned explicitly so a rebuild does not silently move to a new
# Debian release. Pin the digest too if you need byte-identical rebuilds.
FROM python:3.13-slim-bookworm

# Unbuffered so `docker logs` shows output as it happens rather than at exit.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

# confluent-kafka ships manylinux wheels, so no compiler is needed. The old
# image installed gcc and kept it in the final layer.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only what runs. `COPY . .` baked arguments.env -- the file the README
# tells you to put sasl.password in -- straight into the image.
COPY main.py ./
COPY src ./src

RUN useradd --create-home --uid 10001 profiler \
    && mkdir -p /app/data \
    && chown -R profiler:profiler /app/data
USER profiler

# Real defaults only. PRODUCER_CONFIG_FILE and RESULT_DUMP_LOCAL_FILEPATH are
# deliberately left unset: they used to default to the string 'None', which is
# truthy, so the validation meant to catch a missing value never fired.
ENV RUN_INTERVAL=120 \
    T1=IngestionTime \
    T2=consumerWallClockTime \
    T1_UNIT=ms \
    T1_TIMEZONE=utc \
    VALUE_DESERIALIZER=StringDeserializer \
    KEY_DESERIALIZER=StringDeserializer \
    DATE_TIME_FORMAT=epoch

# ENV before CMD: the old file put them after, which reads as though they apply
# to the command rather than the image.
CMD ["python", "main.py"]
