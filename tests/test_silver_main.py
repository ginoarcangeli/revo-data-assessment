import pyspark.sql.types as T
import pytest
from chispa.dataframe_comparer import assert_df_equality
from notebooks.silver_main import load_clean_and_save_silver_data
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    # Create a Spark session for testing
    spark_session = (
        SparkSession.builder.appName("IntegrationTest")
        .config("spark.executorEnv.PYSPARK_PYTHON", "/path/to/python3.9")
        .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/path/to/python3.9")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


@pytest.fixture
def setup_files(spark, tmp_path):
    # Create temporary bronze and geojson files
    bronze_file_path = tmp_path / "bronze.parquet"
    silver_file_path = tmp_path / "silver.parquet"

    # Example data for the bronze parquet file
    bronze_data = [
        {
            "postcode": "1234AB",
            "monetary": "€1,000.00",
            "latitude": 4.897,
            "longitude": 52.377,
        },
        {
            "postcode": None,
            "monetary": "$1,100.00",
            "latitude": 4.898,
            "longitude": 52.378,
        },
    ]
    # Define schema using StructType and StructField
    bronze_schema = T.StructType(
        [
            T.StructField("postcode", T.StringType(), True),
            T.StructField("monetary", T.StringType(), True),
            T.StructField("latitude", T.DoubleType(), True),
            T.StructField("longitude", T.DoubleType(), True),
        ]
    )

    # Use spark to create the DataFrame
    spark.createDataFrame(bronze_data, schema=bronze_schema).write.parquet(
        str(bronze_file_path)
    )

    # Reference the path of the actual GeoJSON file
    geojson_file_path = "../data/geo/post_codes.geojson"

    return str(bronze_file_path), str(geojson_file_path), str(silver_file_path)


def test_load_clean_and_save_silver_data(spark: SparkSession, setup_files: tuple):
    """
    Integration test for the load_clean_and_save_silver_data function.

    This test verifies the end-to-end functionality of the
    load_clean_and_save_silver_data function. It checks whether the
    function correctly loads data from a bronze Parquet file, cleans and fills
    missing postal codes using a GeoJSON file, and saves the processed data to
    a silver Parquet file. The test ensures that the resultant DataFrame matches
    the expected output in terms of data accuracy and format.

    Parameters
    ----------
    spark : SparkSession
        A pytest fixture providing a Spark session for the test.
    setup_files : tuple
        A pytest fixture that sets up temporary file paths for the bronze input file,
        GeoJSON file, and output silver file.

    Example Data
    ------------
    Bronze Input Data:
    | postcode | monetary   | latitude  | longitude |
    |----------|------------|-----------|----------|
    | "1234AB" | "€1,000.00"| 4.897     | 52.377   |
    | None     | "$1,100.00"| 4.898     | 52.378   |

    GeoJSON Data
    Contains postal code boundaries used to fill missing postal codes.

    Expected Silver Output Data:
    | postcode | monetary | latitude  | longitude |
    |----------|----------|-----------|----------|
    | "1234"   | 1000.0   | 4.897     | 52.377   |
    | "1234"   | 1100.0   | 4.898     | 52.378   |

    Steps
    -----
    1. Calls load_clean_and_save_silver_data with the provided paths and Spark session.
    2. Asserts that the function returns True, indicating successful execution.
    3. Loads the resulting silver Parquet file into a DataFrame.
    4. Compares the DataFrame with expected data using chispa's assert_df_equality
        function.

    Assertions
    ----------
    - Verifies that the function executes without error.
    - Ensures the output DataFrame matches the expected data in content and schema.

    Notes
    -----
    - Utilizes the chispa library for robust DataFrame comparison.
    - Temporary files are used for testing to ensure no side effects on actual data.
    """
    bronze_file_path, geojson_path, silver_file_path = setup_files

    # Run the integration test
    success = load_clean_and_save_silver_data(
        bronze_file_path=bronze_file_path,
        geojson_path=geojson_path,
        postal_code_col_name="postcode",
        monetary_col_name="monetary",
        silver_file_path=silver_file_path,
        spark=spark,
    )

    # Check if the function returned True indicating success
    assert success

    # Load the saved silver data
    df_observed = spark.read.parquet(silver_file_path)

    # Define the expected DataFrame
    expected_data = [
        ("1234", 1000.0, 4.897, 52.377),  # Converted and cleaned monetary value.
        (
            "1234",
            1100.0,
            4.898,
            52.378,
        ),  # Filled postal code from geojson and converted monetary.
    ]
    expected_schema = T.StructType(
        [
            T.StructField("postcode", T.StringType(), True),
            T.StructField("monetary", T.DoubleType(), True),
            T.StructField("longitude", T.DoubleType(), True),
            T.StructField("latitude", T.DoubleType(), True),
        ]
    )

    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    # Use chispa to assert DataFrame equality
    assert_df_equality(
        df_observed, expected_df, ignore_nullable=True, ignore_column_order=True
    )
