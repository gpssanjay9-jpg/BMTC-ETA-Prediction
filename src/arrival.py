from pyspark.sql import functions as F
from pyspark.sql.window import Window


def extract_arrivals(snapped_df):
    """
    Extract one arrival per stop.

    Selection priority:
    1. Smallest distance_to_stop
    2. Earliest vehicle_timestamp

    Returns
    -------
    trip_id
    route_id
    sequence
    stop_id
    stop_name
    stop_lat
    stop_lon
    distance_to_stop
    arrival_time
    """

    window = (
        Window
        .partitionBy("trip_id", "sequence")
        .orderBy(
            F.col("distance_to_stop").asc(),
            F.col("vehicle_timestamp").asc()
        )
    )

    arrival_df = (
        snapped_df
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