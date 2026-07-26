from pyspark.sql import functions as F
from pyspark.sql.window import Window


def extract_arrivals(snapped_df):
    """
    Extract the first observed arrival at each stop
    for every trip.
    """

    window = (
        Window
        .partitionBy("trip_id", "sequence")
        .orderBy("vehicle_timestamp")
    )

    arrivals = (
        snapped_df
        .withColumn(
            "rank",
            F.row_number().over(window)
        )
        .filter(F.col("rank") == 1)
        .drop("rank")
    )

    arrivals = arrivals.select(
        "trip_id",
        "route_id",
        "sequence",
        "stop_id",
        "stop_name",
        "vehicle_timestamp"
    )

    arrivals = arrivals.withColumnRenamed(
        "vehicle_timestamp",
        "arrival_time"
    )

    arrivals = arrivals.orderBy(
        "trip_id",
        "sequence"
    )

    return arrivals