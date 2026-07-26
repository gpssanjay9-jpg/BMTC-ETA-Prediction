from pyspark.sql import functions as F
from pyspark.sql.window import Window


def calculate_travel_times(arrival_df):
    """
    Calculate travel time between consecutive observed stops.

    If multiple stop sequences are skipped, the total travel
    time is divided equally among the missing links.

    Output:
        trip_id
        route_id
        from_sequence
        to_sequence
        link_travel_time
    """

    window = (
        Window
        .partitionBy("trip_id")
        .orderBy("sequence")
    )

    df = (
        arrival_df
        .withColumn(
            "next_sequence",
            F.lead("sequence").over(window)
        )
        .withColumn(
            "next_arrival",
            F.lead("arrival_time").over(window)
        )
    )

    df = (
        df
        .filter(F.col("next_sequence").isNotNull())
        .withColumn(
            "sequence_gap",
            F.col("next_sequence") - F.col("sequence")
        )
        .withColumn(
            "total_time",
            F.col("next_arrival") - F.col("arrival_time")
        )
        .filter(F.col("total_time") >= 0)
        .withColumn(
            "link_travel_time",
            F.col("total_time") / F.col("sequence_gap")
        )
    )

    return df.select(
        "trip_id",
        "route_id",
        F.col("sequence").alias("from_sequence"),
        F.col("next_sequence").alias("to_sequence"),
        "sequence_gap",
        "link_travel_time"
    )