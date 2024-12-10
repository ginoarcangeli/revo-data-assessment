import logging

import geopandas as gpd
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.types import StringType
from shapely.geometry import Point

# Configure logging to print to stdout
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Get the logger for the current module
logger = logging.getLogger(__name__)


def get_postal_code_from_coordinates(
    lat: float, lon: float, geo_gdf: gpd.GeoDataFrame
) -> str:
    """
    Returns the postal code for a given set of latitude and longitude coordinates.

    Parameters
    ----------
    lat : float
        Latitude of the point.
    lon : float
        Longitude of the point.
    geo_gdf : GeoDataFrame
        A GeoDataFrame containing PC4 code boundaries.

    Returns
    -------
    str
        The postal code if the point is within any of the polygons, otherwise None.
    """
    # Create a point from the latitude and longitude
    point = Point(lon, lat)
    logger.info(f"Point created for lon: {lon} lat: {lat}")

    # Iterate through polygons to find which contains the point
    for _, row in geo_gdf.iterrows():
        if row["geometry"].contains(point):
            logger.info(f"Found postal code: {row['properties']['pc4_code']}")
            return row["properties"]["pc4_code"]

    return None


def get_missing_postal_codes(
    full_df: SparkDataFrame, postal_code_col_name: str, geojson_path: str
) -> SparkDataFrame:
    """
    Updates missing postal codes in a DataFrame by using geographical coordinates
    to determine the correct postal code from a GeoJSON file.

    This function identifies rows where the postal code is missing or NaN and uses
    a user-defined function (UDF) to look up the postal code based on latitude and
    longitude coordinates. The GeoJSON file provides the boundary data for postal
    codes.

    Parameters
    ----------
    full_df : SparkDataFrame
        The input DataFrame containing the data with postal codes and coordinates.
    postal_code_col_name : str
        The name of the column in the DataFrame that contains postal codes.
    geojson_path : str
        The file path to the GeoJSON file that contains postal code boundary data.

    Returns
    -------
    SparkDataFrame
        A Spark DataFrame with the missing postal codes filled in using the
        geographical data from the GeoJSON file.

    Notes
    -----
    - The function assumes that the DataFrame has 'longitude' and 'latitude' columns
      to use for geolocation.
    - Requires the `geopandas` and `shapely` libraries to be installed and accessible.
    - The calculation of postal codes is done using a UDF for compatibility with
      Spark DataFrames.
    """
    # Create the GeoJSON object
    geo_gdf = gpd.read_file(geojson_path)

    # Define a UDF that wraps the existing function
    def udf_get_postal_code(lat, lon):
        return get_postal_code_from_coordinates(lat=lat, lon=lon, geo_gdf=geo_gdf)

    # Register the UDF with Spark
    postal_code_udf = F.udf(udf_get_postal_code, StringType())

    # Define the conditions to update the missing value
    is_postal_code_col_null = F.col(postal_code_col_name).isNull()
    is_postal_code_col_nan = F.isnan(F.col(postal_code_col_name))

    df_missing = full_df.filter(is_postal_code_col_null | is_postal_code_col_nan)
    df_complete = full_df.filter(~is_postal_code_col_null | ~is_postal_code_col_nan)

    logger.info(f"Records with incomplete postal codes: {df_missing.count()}")

    # Use when to conditionally update the postal code column
    logger.info("Getting missing postal codes ...")
    df_filled = df_missing.withColumn(
        postal_code_col_name, postal_code_udf(F.col("longitude"), F.col("latitude"))
    )

    return df_complete.unionByName(df_filled)


def convert_int_to_float(
    df: SparkDataFrame,
    int_col_name: str,
    new_col_name: str = None,
    target_type: str = "double",
) -> SparkDataFrame:
    """
    Converts an integer type column in a DataFrame to a float or double type column.

    Parameters
    ----------
    df : SparkDataFrame
        The input Spark DataFrame containing the integer column.
    int_col_name : str
        The name of the integer column to convert.
    new_col_name : str, optional
        The name for the new column with the converted type.
        If None, the original column will be replaced.
    target_type : str, optional
        The target type for the conversion, either "float" or "double".
        Default is "double".

    Returns
    -------
    SparkDataFrame
        A Spark DataFrame with the specified column converted to float or double type.
    """
    if new_col_name is None:
        new_col_name = int_col_name

    # Cast the integer column to the specified type
    logger.info("Converting int column to float column ...")
    return df.withColumn(new_col_name, F.col(int_col_name).cast(target_type))


def clean_postal_code_column(
    df: SparkDataFrame, col_name: str, new_col_name: str = None
) -> SparkDataFrame:
    """
    Cleans the specified column by removing all characters except numbers and capital
    letters.

    Parameters
    ----------
    df : SparkDataFrame
        The input DataFrame containing the column to clean.
    col_name : str
        The name of the column to clean.
    new_col_name : str, optional
        The name for the new column where the cleaned data will be stored.
        If None, the original column will be replaced.

    Returns
    -------
    SparkDataFrame
        A DataFrame with the specified column cleaned to contain only numbers and
        capital letters.
    """
    if new_col_name is None:
        new_col_name = col_name

    # Define the regex pattern to match characters that are not numbers
    # since we are joining on only the PC4 ciphers
    pattern = r"[^0-9]"

    # Use regexp_replace to remove unwanted characters
    logger.info("Cleaning postal code column ...")
    return df.withColumn(new_col_name, F.regexp_replace(F.col(col_name), pattern, ""))


def clean_and_fill_data(
    df: SparkDataFrame,
    geojson_path: str,
    postal_code_col_name: str,
    monetary_col_name: str,
) -> SparkDataFrame:
    """
    Cleans and completes the data in a DataFrame by performing several operations:
    - Cleans the postal code and monetary value columns.
    - Fills missing postal codes using a GeoJSON file.
    - Drops any remaining unusable records.

    Parameters
    ----------
    df : SparkDataFrame
        The input DataFrame containing the data to be cleaned and completed.
    geojson_path : str
        The file path to the GeoJSON file used to fill missing postal codes.
    postal_code_col_name : str
        The name of the column in the DataFrame that contains postal codes.
    monetary_col_name : str
        The name of the column in the DataFrame that contains monetary values.

    Returns
    -------
    SparkDataFrame
        A cleaned and completed DataFrame with usable data ready for further analysis.
    """
    # Select only the relevant columns
    df_select = df.select(
        [monetary_col_name, postal_code_col_name, "longitude", "latitude"]
    )
    # Clean the postal code column
    logger.info("Cleaning postal code column...")
    df_clean_zipcode = clean_postal_code_column(
        df=df_select, col_name=postal_code_col_name
    )

    logger.info("Validating postcodes ...")
    df_validated_zipcode = validate_dutch_post_codes(
        df=df_clean_zipcode, column_name=postal_code_col_name
    )
    logger.info(
        f"Postcodes successfully validated for {df_validated_zipcode.count()} records!"
    )

    # Clean the monetary column
    df_clean_money = clean_and_convert_monetary_col(
        df=df_validated_zipcode, column_name=monetary_col_name
    )

    # Convert monetary values from integer to float
    df_clean = convert_int_to_float(df=df_clean_money, int_col_name=monetary_col_name)
    logger.info(f"Monetary columns cleaned for {df_clean.count()} successfully!")

    # Fill in missing postal codes using the GeoJSON file
    logger.info("Filling missing and invalid postcodes...")
    df_complete = get_missing_postal_codes(
        full_df=df_clean,
        postal_code_col_name=postal_code_col_name,
        geojson_path=geojson_path,
    )
    logger.info(
        f"Filled missing and invalid postal codes for {df_complete.count()} records!"
    )

    # Drop unusable records (e.g., those with null or erroneous values)
    logger.info("Dropping unusable records...")
    df_final = df_complete.dropna(subset=[monetary_col_name, postal_code_col_name])
    logger.info(f"Remaining usable records: {df_final.count()}")
    return df_final


def validate_dutch_post_codes(df: SparkDataFrame, column_name: str) -> SparkDataFrame:
    """
    Validates Dutch postcodes in a DataFrame column and sets invalid entries to null.

    Parameters
    ----------
    df : SparkDataFrame
        The input DataFrame containing the postal codes to validate.
    column_name : str
        The name of the column containing the postcodes to be validated.

    Returns
    -------
    SparkDataFrame
        A DataFrame with the specified column updated to null where postcodes do not
        match the Dutch format (PC4 code).
    """
    # Define the regex pattern for Dutch postal codes (e.g., 1234AB)
    pattern = r"^\d{4}$"

    # Use regexp_extract to check if the postcode matches the pattern
    return df.withColumn(
        column_name,
        F.when(
            F.regexp_extract(F.col(column_name), pattern, 0) == "",
            F.lit(None),
        ).otherwise(F.col(column_name)),
    )


def clean_and_convert_monetary_col(df: SparkDataFrame, column_name: str):
    """
    Cleans and converts a monetary column in a DataFrame to a float type.

    This function processes a specified monetary column in a Spark DataFrame by removing
    non-numeric characters (except for periods and commas), adjusts the format for
    decimal conversion, and casts the column to a float type. It is particularly
    useful for cleaning currency data that may include symbols, spaces, or
    other formatting characters.

    Parameters
    ----------
    df : SparkDataFrame
        The input DataFrame containing the monetary column to be cleaned and converted.
    column_name : str
        The name of the column to process and convert to a float type.

    Returns
    -------
    SparkDataFrame
        A new DataFrame with the specified column cleaned of non-numeric characters and
        converted to a float type for numerical analysis.

    Notes
    -----
    - Non-numeric characters are removed using a regular expression, keeping only digits,
      commas, and periods.
    - Commas are replaced with periods to standardize decimal notation before conversion.
    - The function assumes that the input column may contain various currency symbols and
      formatting, which will be stripped away during processing.
    """
    # Remove special characters and convert to a standard decimal format if necessary
    cleaned_df = (
        df.withColumn(
            column_name,
            F.regexp_replace(
                F.col(column_name), r"[^0-9,.]", ""
            ),  # Remove non-numeric characters except comma/period
        )
        .withColumn(
            column_name,
            F.regexp_replace(
                F.col(column_name), ",", "."
            ),  # Replace comma with period if needed for decimal conversion
        )
        .withColumn(
            column_name,
            F.col(column_name).cast("float"),  # Convert the cleaned string to a float
        )
    )
    return cleaned_df
