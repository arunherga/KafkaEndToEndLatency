import csv
import logging
import os

from src.core.statistics import RESULT_FIELDS

logger = logging.getLogger(__name__)


def write_to_csv(file_location: str, rows: list) -> None:
    """Append result rows to a CSV file, writing the header only on creation.

    This used to overwrite. Combined with `restart: unless-stopped` in
    docker-compose, every restart destroyed the previous window's results --
    the opposite of what a profiler is for.
    """
    try:
        directory = os.path.dirname(os.path.abspath(file_location))
        os.makedirs(directory, exist_ok=True)

        is_new = not os.path.exists(file_location) or os.path.getsize(file_location) == 0
        with open(file_location, 'a', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(fh, fieldnames=RESULT_FIELDS)
            if is_new:
                writer.writeheader()
            writer.writerows(rows)

        logger.info(f"Appended {len(rows)} row(s) of latency measurements to {file_location}")
    except Exception as e:
        logger.error(f"Error writing to CSV file {file_location}: {str(e)}")
        raise


def output_to_file(config, rows: list) -> None:
    """Output results to a local file."""
    # The path is used as given, relative to the working directory. It used to
    # be forced under /app/data, which broke every non-Docker run.
    write_to_csv(config.local_filepath, rows)
