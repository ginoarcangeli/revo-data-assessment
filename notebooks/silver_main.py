import logging

from notebooks.utils.silver_utils import clean_and_fill_data
from pyspark.sql import SparkSession

# Configure logging to print to stdout
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Get the logger for the current module
logger = logging.getLogger(__name__)


def load_clean_and_save_silver_data(
    bronze_file_path: str,
    geojson_path: str,
    postal_code_col_name: str,
    monetary_col_name: str,
    silver_file_path: str,
    spark: SparkSession,
    write_mode: str = "overwrite",
    format: str = "parquet",
) -> bool:
    """
    Loads, cleans, and saves data from a bronze parquet file to a silver location.

    This function reads raw data from the specified bronze file path, processes it to
    clean and fill missing values using the provided GeoJSON path for postal code
    information, and then writes the cleaned data to the specified silver file path
    in the chosen format.

    Parameters
    ----------
    bronze_file_path : str
        The file path of the raw bronze data to be loaded.
    geojson_path : str
        The file path to the GeoJSON file used for filling missing postal codes.
    postal_code_col_name : str
        The name of the column containing postal codes in the data.
    monetary_col_name : str
        The name of the column containing monetary values in the data.
    silver_file_path : str
        The file path where the cleaned silver data will be saved.
    spark : SparkSession
        The Spark session used for reading and writing data.
    write_mode : str, optional
        The mode for writing the data (e.g., 'overwrite', 'append'),
        by default "overwrite".
    format : str, optional
        The format in which to save the processed data (e.g., 'parquet', 'csv'),
        by default "parquet".

    Returns
    -------
    bool
        Returns True if the process completes successfully, False if an error occurs.
    """
    try:
        # Load the raw data from the bronze parquet file
        df_in = spark.read.parquet(bronze_file_path)
        logger.info(
            f"Loaded {df_in.count()} records from {bronze_file_path} successfully!"
        )

        # Clean and fill the data by using a utility function
        df_silver = clean_and_fill_data(
            df=df_in,
            geojson_path=geojson_path,
            postal_code_col_name=postal_code_col_name,
            monetary_col_name=monetary_col_name,
        )
        logger.info(f"Cleaned and filled {df_silver.count()} successfully!")

        # Write the cleaned data to the silver file path
        df_silver.write.mode(write_mode).format(format).save(silver_file_path)
        logger.info(f"Clean files saved {df_silver.count()} successfully!")
        return True

    except Exception as e:
        # Log any exceptions that occur during the process
        logger.error(f"An error occurred: {e}")
        return False
