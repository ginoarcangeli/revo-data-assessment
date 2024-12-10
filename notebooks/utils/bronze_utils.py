import logging

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Configure logging to print to stdout
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Get the logger for the current module
logger = logging.getLogger(__name__)


def ingest_parquet_data_from_raw_file(
    raw_file_path: str,
    output_file_path: str,
    file_type: str,
    spark: SparkSession,
    write_mode: str = "overwrite",
    format: str = "parquet",
) -> bool:
    """
    Ingests data from a raw file (CSV or JSON) and writes it to a specified format
    (default is Parquet).

    Parameters
    ----------
    raw_file_path : str
        The file path to the raw input data file, either in CSV or JSON format.
    output_file_path : str
        The destination file path where the processed data will be saved.
    file_type : str
        The type of the raw input file. Expected values are 'csv' or 'json'.
    spark : SparkSession
        The SparkSession object used to perform read and write operations.
    write_mode : str, optional
        The mode to use when writing data to the output file. Default is "overwrite".
        Options include 'overwrite', 'append', 'ignore', and 'error' (default behavior).
    format : str, optional
        The format in which to save the output data. Default is 'parquet'.

    Returns
    -------
    bool
        Returns True if the data was successfully read and written to the output file.
        Returns False if an error occurred during the process.

    Raises
    ------
    ValueError
        Raised if the file_type is not supported. Supported file types are
        'csv' and 'json'.

    Notes
    -----
    This function uses PySpark to read data from raw input files and writes it out in a
    specified format. The logging module is used to log the status of operations and any
    errors encountered.
    """
    try:
        if file_type == "csv":
            # Read and save the file
            df = spark.read.csv(raw_file_path, header=True, inferSchema=True)
            df.write.mode(write_mode).format(format).save(output_file_path)
            logger.info("CSV file saved successfully!")
            return True
        elif file_type == "json":
            # Read and save the file
            df = spark.read.json(raw_file_path)
            df.write.mode(write_mode).format(format).save(output_file_path)
            logger.info("JSON file saved successfully!")
            return True
        else:
            raise ValueError("File type not supported. Must be: 'csv' or 'json'.")
    except AnalysisException as e:
        # Handle specific file-related exceptions
        logger.error(f"An error occurred while loading the CSV file: {e}")
    except Exception as e:
        # Handle any other exceptions
        logger.error(f"An unexpected error occurred: {e}")

    return False
