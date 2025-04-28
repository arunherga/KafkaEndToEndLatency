import os
import logging
import pandas as pd
from typing import List

logger = logging.getLogger(__name__)

def write_to_csv(file_location: str, data: pd.DataFrame) -> None:
    """Write data to a CSV file."""
    try:
        output_dir = '/app/data'
        output_file = os.path.join(output_dir, file_location)
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        data.to_csv(output_file, index=False)
        logger.info(f"Latency measurements written to {file_location} successfully")
    except Exception as e:
        logger.error(f"Error writing to CSV file {file_location}: {str(e)}")
        raise

def output_to_file(config, avg: float, quantiles: List[float], date_string: str) -> None:
    """Output results to a local file."""
    try:
        df = pd.DataFrame({
            'average': [avg],
            'quantile_50': [quantiles[0]],
            'quantile_90': [quantiles[1]],
            'quantile_95': [quantiles[2]],
            'quantile_99': [quantiles[3]],
            'quantile_99.9': [quantiles[4]],
            'date_time': [date_string]
        })
        write_to_csv(config.local_filepath, df)
    except Exception as e:
        logger.error(f"Error outputting to file: {str(e)}")
        raise 