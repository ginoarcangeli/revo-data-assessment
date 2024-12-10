import logging

from pyspark.sql import SparkSession
from utils.gold_utils import (
    calculate_average_revenue_per_postcode_per_night,
    get_revenue_comparison_insight_columns,
)

# Configure logging to print to stdout
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Get the logger for the current module
logger = logging.getLogger(__name__)


def generate_revenue_insights_and_save_gold_data(
    airbnb_silver_file_path: str,
    rentals_silver_file_path: str,
    gold_file_path: str,
    spark: SparkSession,
    write_mode: str = "overwrite",
    format: str = "parquet",
) -> bool:
    """
    Generates revenue insights from silver data and saves the results as gold data.

    This function loads Airbnb and rental data from their respective silver-level
    data paths, calculates average revenue per postcode per night, and generates
    additional revenue insights. The enriched dataset is then saved to the specified
    gold-level data path in the desired format.

    Parameters
    ----------
    airbnb_silver_file_path : str
        The file path to the silver-level Airbnb data, stored in Parquet format.
    rentals_silver_file_path : str
        The file path to the silver-level rental data, stored in Parquet format.
    gold_file_path : str
        The destination file path for storing the gold-level data, including revenue
        insights.
    spark : SparkSession
        The Spark session used for data processing.
    write_mode : str, optional
        The mode for writing the output data (e.g., 'overwrite', 'append'), by default
        "overwrite".
    format : str, optional
        The format in which to save the output data (e.g., 'parquet', 'csv'), by default
        "parquet".

    Returns
    -------
    bool
        Returns True if the process completes successfully, False if an error occurs.

    Notes
    -----
    The function assumes that the input data files are accessible and correctly
    formatted as Parquet files, and it relies on utility functions
    `calculate_average_revenue_per_postcode_per_night` and
    `get_revenue_comparison_insight_columns` to compute revenue metrics and insights.
    """
    try:
        # Load the raw data from the silver parquet files
        df_airbnb = spark.read.parquet(airbnb_silver_file_path)
        logger.info(f"File loaded from {airbnb_silver_file_path} successfully!")

        df_rentals = spark.read.parquet(rentals_silver_file_path)
        logger.info(f"File loaded from {rentals_silver_file_path} successfully!")

        # Calculate the average revenue per postcode per night
        df_gold = calculate_average_revenue_per_postcode_per_night(
            df_rentals=df_rentals, df_airbnb=df_airbnb
        )
        logger.info("Successfully calculated average revenue per postcode per night!")

        # Generate revenue insights
        df_gold_with_insights = get_revenue_comparison_insight_columns(
            average_revenue_df=df_gold
        )

        # Save the enriched data to the gold file path
        df_gold_with_insights.write.mode(write_mode).format(format).save(gold_file_path)
        logger.info("Revenue insights saved successfully!")
        return True

    except Exception as e:
        # Log any exceptions that occur during the process
        logger.error(f"An error occurred: {e}")
        return False
