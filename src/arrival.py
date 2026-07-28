from pyspark.sql import functions as F
from pyspark.sql.window import Window


def extract_arrivals(df):
    """
    Extract one arrival for every stop in a trip.

    Output:
        trip_id
        route_id
        sequence
        stop_id
        stop_name
        arrival_time
    """

    # Earliest vehicle observation for each stop
    window = (
        Window
        .partitionBy("trip_id", "sequence")
        .orderBy("vehicle_timestamp")
    )

    arrival_df = (
        df
        .withColumn(
            "rank",
            F.row_number().over(window)
        )
        .filter(F.col("rank") == 1)
        .drop("rank")
        .select(
            "trip_id",
            "route_id",
            "sequence",
            "stop_id",
            "stop_name",
            "stop_lat",
            "stop_lon",
            "distance_to_stop",
            F.col("vehicle_timestamp").alias("arrival_time")
        )
        .orderBy(
            "trip_id",
            "sequence"
        )
    )

    return arrival_df
