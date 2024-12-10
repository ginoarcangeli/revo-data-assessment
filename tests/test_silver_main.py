import json
import shutil

import pytest
from chispa.dataframe_comparer import assert_df_equality
from notebooks.silver_main import (
    load_clean_and_save_silver_data,
)  # Adjust import based on your module structure
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    # Create a Spark session for testing
    spark_session = (
        SparkSession.builder.appName("IntegrationTest").master("local[2]").getOrCreate()
    )
    yield spark_session
    spark_session.stop()


@pytest.fixture
def setup_files(tmp_path):
    # Create temporary bronze and geojson files
    bronze_file_path = tmp_path / "bronze.parquet"
    geojson_file_path = tmp_path / "geo.json"
    silver_file_path = tmp_path / "silver.parquet"

    # Example data for the bronze parquet file
    bronze_data = [
        {
            "postcode": "1234AB",
            "monetary": "€1,000.00",
            "longitude": 4.897,
            "latitude": 52.377,
        },
        {
            "postcode": None,
            "monetary": "$1,100.00",
            "longitude": 4.898,
            "latitude": 52.378,
        },
    ]
    bronze_schema = (
        "postcode STRING, monetary STRING, longitude DOUBLE, latitude DOUBLE"
    )
    spark.createDataFrame(bronze_data, schema=bronze_schema).write.parquet(
        str(bronze_file_path)
    )

    # Example geojson data
    geojson_content = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"pc4_code": "1234"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [4.896, 52.376],
                            [4.899, 52.376],
                            [4.899, 52.379],
                            [4.896, 52.379],
                            [4.896, 52.376],
                        ]
                    ],
                },
            }
        ],
    }
    with open(geojson_file_path, "w") as geojson_file:
        json.dump(geojson_content, geojson_file)

    return str(bronze_file_path), str(geojson_file_path), str(silver_file_path)


def test_load_clean_and_save_silver_data(spark, setup_files):
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
    df_silver = spark.read.parquet(silver_file_path)

    # Define the expected DataFrame
    expected_data = [
        ("1234AB", 1000.0, 4.897, 52.377),  # Converted and cleaned monetary value.
        (
            "1234",
            1100.0,
            4.898,
            52.378,
        ),  # Filled postal code from geojson and converted monetary.
    ]
    expected_schema = (
        "postcode STRING, monetary DOUBLE, longitude DOUBLE, latitude DOUBLE"
    )
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    # Use chispa to assert DataFrame equality
    assert_df_equality(df_silver, expected_df, ignore_nullable=True)

    # Clean up files if needed (e.g., in non-temporary testing environments)
    shutil.rmtree(silver_file_path, ignore_errors=True)
