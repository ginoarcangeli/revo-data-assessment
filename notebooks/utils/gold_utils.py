import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def calculate_average_revenue_per_postcode_per_night(
    df_rentals: DataFrame, df_airbnb: DataFrame
) -> DataFrame:
    """
    Calculates the average revenue per postcode by joining two DataFrames.

    Parameters
    ----------
    df_rentals : DataFrame
        The first DataFrame containing columns "postalCode" and "rent" (monthly).
    df_airbnb : DataFrame
        The second DataFrame containing columns "zipcode" and "price" (per night).

    Returns
    -------
    DataFrame
        A DataFrame with the average revenue per postcode.
    """
    # Select relevant columns from each DataFrame
    df_rentals_selected = df_rentals.select(
        F.col("postalCode"), (F.col("rent") / 30).alias("rent_per_night")
    )
    df_airbnb_selected = df_airbnb.select(F.col("zipcode"), F.col("price"))

    # Join the DataFrames on "zipcode" and "postCode"
    joined_df = df_rentals_selected.join(
        df_airbnb_selected,
        df_rentals_selected["postalCode"] == df_airbnb_selected["zipcode"],
        how="fullouter",
    )

    # Calculate average revenue per postcode
    df_output = joined_df.groupBy("zipcode").agg(
        F.avg("rent_per_night").alias("avg_per_night_long_term"),
        F.avg("price").alias("avg_per_night_short_term"),
    )

    return df_output.withColumnRenamed("zipcode", "pc_4_code")


def get_revenue_comparison_insight_columns(average_revenue_df: DataFrame) -> DataFrame:
    """
    Enhances a DataFrame with revenue comparison insights and orders the result by total
    average revenue.

    This function adds several columns to the input DataFrame to provide insights into
    the profitability of short-term versus long-term rentals, calculates the total
    average revenue per night, and orders the DataFrame by this total average revenue
    in descending order.

    Parameters
    ----------
    average_revenue_df : DataFrame
        A Spark DataFrame containing average nightly revenues with columns
        "avg_per_night_short_term" and "avg_per_night_long_term".

    Returns
    -------
    DataFrame
        A Spark DataFrame enhanced with additional columns for revenue comparison:
        - "is_short_term_more_profitable": Boolean if short-term rental more profitable.
        - "is_long_term_more_profitable": Boolean if long-term rental more profitable.
        - "is_equally_profitable": Boolean if both rental types are equally profitable.
        - "total_avg_per_night": Average of short-term and long-term nightly revenues.

        The DataFrame is ordered by "total_avg_per_night" in descending order.
    """
    # Add a column indicating if short-term rental is more profitable than
    # long-term rental
    result_df = average_revenue_df.withColumn(
        "is_short_term_more_profitable",
        F.col("avg_per_night_short_term") > F.col("avg_per_night_long_term"),
    )

    # Add a column indicating if long-term rental is more profitable than
    # short-term rental
    result_df = result_df.withColumn(
        "is_long_term_more_profitable",
        F.col("avg_per_night_long_term") > F.col("avg_per_night_short_term"),
    )

    # Add a column indicating if both rental types are equally profitable
    result_df = result_df.withColumn(
        "is_equally_profitable",
        F.col("avg_per_night_long_term") == F.col("avg_per_night_short_term"),
    )

    # Calculate the total average per night by averaging
    # long-term and short-term revenues
    result_df = result_df.withColumn(
        "total_avg_per_night",
        (F.col("avg_per_night_long_term") + F.col("avg_per_night_short_term")) / 2,
    )

    # Order the DataFrame by the total average per night in descending order for
    # better insights
    return result_df.orderBy(F.col("total_avg_per_night").desc())
