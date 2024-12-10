import pytest
from chispa.dataframe_comparer import assert_df_equality
from notebooks.utils.silver_utils import clean_postal_code_column
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


@pytest.fixture(scope="module")
def spark():
    # Create a Spark session for testing
    spark_session = (
        SparkSession.builder.appName("UnitTest")
        .config("spark.executorEnv.PYSPARK_PYTHON", "/path/to/python3.9")
        .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/path/to/python3.9")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_clean_postal_code_column(spark: SparkSession):
    """
    Tests the clean_postal_code_column function to ensure it correctly cleans
    postal codes by removing unwanted characters, leaving only numbers and
    capital letters.

    Parameters
    ----------
    spark : SparkSession
        The Spark session used to create DataFrames for testing.

    Examples
    --------
    Input:
    A DataFrame with a column "raw_postcode" containing various unwanted characters:

    | raw_postcode |
    |--------------|
    | "1234-AB"    |
    | "5678 CD"    |
    | "9@01!2EF"   |
    | "0000-zz"    |

    Expected output:
    A DataFrame with the "raw_postcode" column cleaned to contain only numbers and
    capital letters:

    | raw_postcode |
    |--------------|
    | "1234AB"     |
    | "5678CD"     |
    | "9012EF"     |
    | "0000"       |

    This function uses chispa's `assert_df_equality` to compare the expected result
    with the function's output, ensuring that the clean_postal_code_column function
    behaves as expected.
    """
    data = [("1234-AB",), ("5678 CD",), ("9@01!2EF",), ("0000-zz",)]
    columns = ["raw_postcode"]
    df = spark.createDataFrame(data, columns)

    # Apply the function to clean the postal code column
    result_df = clean_postal_code_column(df, "raw_postcode")

    # Define expected DataFrame schema and data
    expected_data = [("1234",), ("5678",), ("9012",), ("0000",)]
    expected_schema = StructType([StructField("raw_postcode", StringType(), True)])
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Use chispa to assert DataFrame equality
    assert_df_equality(result_df, expected_df)
