import time
import logging
import numpy as np
from datetime import datetime
from confluent_kafka import Consumer

from src.config.config_manager import create_kafka_config, read_ccloud_config
from src.core.message_processor import MessageProcessor
from src.output.kafka_output import output_to_kafka
from src.output.file_output import output_to_file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_results(processor: MessageProcessor, config) -> None:
    """Process and output the latency results."""
    n = datetime.now()
    date_string = n.strftime("%Y-%m-%d %H:%M:%S.%f")
    
    logger.info(f"Total Message read by consumer: {processor.count}")
    logger.info(f"Current Time: {date_string}")
    
    if config.enable_sampling:
        length = int(len(processor.latency_array) * 0.3)
        random_elements = random.sample(processor.latency_array, length)
        avg = sum(random_elements) // len(random_elements)
        logger.info(f"Number of message sampled(sampling enabled): {len(random_elements)}")
    else:
        avg = sum(processor.latency_array) // processor.count
        logger.info(f"Number of messages sampled(sampling disabled): {processor.count}")
    
    logger.info(f"Average Latency in ms: {avg}")
    
    quantiles = np.quantile(processor.latency_array, [.5, .9, .95, .99, .999])
    logger.info("\nQuantiles of the latencies measured in ms:")
    logger.info(f"50th percentile: {quantiles[0]}")
    logger.info(f"90th percentile: {quantiles[1]}")
    logger.info(f"95th percentile: {quantiles[2]}")
    logger.info(f"99th percentile: {quantiles[3]}")
    logger.info(f"99.9th percentile: {quantiles[4]}")
    
    if config.output_type == 'dumpToTopic':
        output_to_kafka(config, avg, quantiles, date_string)
    elif config.output_type == 'localFileDump':
        output_to_file(config, avg, quantiles, date_string)

def main():
    try:
        config = create_kafka_config()
        processor = MessageProcessor(config)
        
        consumer = Consumer(read_ccloud_config(config.consumer_config_file))
        consumer.subscribe([config.input_topic])
        
        logger.info("Consumer has started!")
        
        start_time = time.time()
        elapsed_time = 0
        
        while elapsed_time < config.run_interval:
            msg = consumer.poll(1.0)
            if msg is not None:
                processor.process_message(msg)
            elapsed_time = time.time() - start_time
            
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
    finally:
        if 'consumer' in locals():
            consumer.close()
        if 'processor' in locals():
            process_results(processor, config)
        logger.info("Consumer closing")

if __name__ == '__main__':
    main() 