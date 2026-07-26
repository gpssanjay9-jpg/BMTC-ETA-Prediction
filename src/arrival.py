from pyspark.sql import functions as F
from pyspark.sql.window import Window


def extract_arrivals(df):
    """
    Extract one arrival timestamp per trip and stop sequence.

    Input:
        Snapped GPS data

    Output:
        One record per:
        trip_id + sequence

    Logic:
        Earliest timestamp at a stop is considered arrival time.
    """

    window = (
        Window
        .partitionBy(
            "trip_id",
            "sequence"
        )
        .orderBy(
            F.col("vehicle_timestamp").asc()
        )
    )

    arrivals = (
        df
        .withColumn(
            "rank",
            F.row_number().over(window)
        )
        .filter(
            F.col("rank") == 1
        )
        .drop("rank")
    )

    return arrivals.select(
        "trip_id",
        "route_id",
        "sequence",
        "stop_id",
        "stop_name",
        "vehicle_timestamp",
        "distance_to_stop"
    )