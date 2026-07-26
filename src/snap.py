from pyspark.sql import functions as F
from pyspark.sql.window import Window

EARTH_RADIUS = 6371000  # meters


def join_candidate_stops(gtfs_df, stop_df):

    gtfs_df = gtfs_df.drop("stop_id")

    return gtfs_df.join(
        stop_df,
        on="route_id",
        how="inner"
    )


def calculate_distance(df):
    """
    Calculate Haversine distance (meters)
    between the GPS point and candidate stop.
    """

    lat1 = F.radians(F.col("latitude"))
    lon1 = F.radians(F.col("longitude"))

    lat2 = F.radians(F.col("stop_lat"))
    lon2 = F.radians(F.col("stop_lon"))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        F.pow(F.sin(dlat / 2), 2)
        + F.cos(lat1)
        * F.cos(lat2)
        * F.pow(F.sin(dlon / 2), 2)
    )

    c = 2 * F.asin(F.sqrt(a))

    return df.withColumn(
        "distance_to_stop",
        F.lit(EARTH_RADIUS) * c
    )


def select_nearest_stop(df):
    """
    For each GPS observation, keep only the
    nearest stop on that route.
    """

    window = (
        Window
        .partitionBy(
            "trip_id",
            "vehicle_timestamp"
        )
        .orderBy(
            F.col("distance_to_stop").asc()
        )
    )

    return (
        df.withColumn(
            "rank",
            F.row_number().over(window)
        )
        .filter(F.col("rank") == 1)
        .drop("rank")
    )


def snap_gtfs(gtfs_df, stop_df):
    """
    Complete snapping pipeline.
    """

    df = join_candidate_stops(
        gtfs_df,
        stop_df
    )

    df = calculate_distance(df)

    df = select_nearest_stop(df)

    return df