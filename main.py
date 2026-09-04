import logging
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer

from src.config.config_manager import create_kafka_config, read_ccloud_config
from src.core.message_processor import MessageProcessor
from src.output.file_output import output_to_file
from src.output.kafka_output import output_to_kafka

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def log_results(rows: list) -> None:
    """Log the aggregate figures, then the per-partition breakdown."""
    overall = rows[-1]
    logger.info(
        f"Latency over {overall['count']} messages (ms): "
        f"min={overall['min_ms']:.1f} mean={overall['mean_ms']:.1f} "
        f"max={overall['max_ms']:.1f} stddev={overall['stddev_ms']:.1f}"
    )
    logger.info(
        f"  p50={overall['p50_ms']:.1f} p90={overall['p90_ms']:.1f} "
        f"p95={overall['p95_ms']:.1f} p99={overall['p99_ms']:.1f} "
        f"p99.9={overall['p999_ms']:.1f}"
    )

    partitions = rows[:-1]
    if len(partitions) > 1:
        # One slow partition is invisible in a single topic-wide p99.
        logger.info("Per-partition latency (ms):")
        for row in partitions:
            logger.info(
                f"  partition {row['partition']}: n={row['count']} "
                f"p50={row['p50_ms']:.1f} p99={row['p99_ms']:.1f} max={row['max_ms']:.1f}"
            )

def process_results(processor: MessageProcessor, config, window_seconds=None) -> None:
    """Report and output the latency results."""
    date_string = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    logger.info(f"Current Time: {date_string}")
    # Report coverage and clock-skew diagnostics before any numbers, including
    # on an empty run -- that is exactly when they explain what went wrong.
    processor.log_diagnostics()

    if processor.stats.count == 0:
        if processor.negative_latencies:
            logger.warning(
                f"All {processor.negative_latencies} measured latencies were negative, so there "
                "is nothing to report. T1 and T2 come from different hosts; check their clocks."
            )
        else:
            logger.warning(
                f"No messages with usable timestamps were read from topic '{config.input_topic}' "
                f"during the {config.run_interval}s window, so there are no latency results to "
                "report. Check INPUT_TOPIC, the consumer group offsets and that the topic is live."
            )
        return

    context = {
        'date_time': date_string,
        'topic': config.input_topic,
        'group_id': config.group_id,
        'skipped': processor.skipped_total,
        'negative_latencies': processor.negative_latencies,
        'window_seconds': round(window_seconds, 3) if window_seconds is not None else None,
    }
    rows = processor.stats.rows(context)
    log_results(rows)

    if config.output_type == 'dumpToTopic':
        output_to_kafka(config, rows)
    elif config.output_type == 'localFileDump':
        output_to_file(config, rows)

def main():
    consumer = None
    config = None
    processor = None
    window_seconds = None
    try:
        config = create_kafka_config()
        processor = MessageProcessor(config)

        consumer_config = read_ccloud_config(config.consumer_config_file)
        if config.group_id:
            consumer_config['group.id'] = config.group_id
        if not consumer_config.get('group.id'):
            raise ValueError(
                "No consumer group configured: set the GROUP_ID environment variable "
                f"or add a group.id property to {config.consumer_config_file}"
            )

        consumer = Consumer(consumer_config)
        consumer.subscribe([config.input_topic])

        logger.info("Consumer has started!")

        start_time = time.time()
        elapsed_time = 0

        while elapsed_time < config.run_interval:
            msg = consumer.poll(1.0)
            if msg is not None:
                processor.process_message(msg)
            elapsed_time = time.time() - start_time

        window_seconds = elapsed_time

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
    finally:
        if consumer is not None:
            consumer.close()
        if processor is not None and config is not None:
            # Never let a reporting failure mask the error that got us here.
            try:
                process_results(processor, config, window_seconds)
            except Exception as e:
                logger.error(f"Error reporting latency results: {str(e)}")
        logger.info("Consumer closing")

if __name__ == '__main__':
    main()
