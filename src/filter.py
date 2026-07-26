from pyspark.sql import functions as F


def filter_schedule(df):
    """
    Keep only SCHEDULED and ADDED trips.
    """

    return df.filter(
        F.col("schedule_relationship").isin(
            "SCHEDULED",
            "ADDED"
        )
    )


def filter_valid_routes(gtfs_df, stop_df):
    """
    Keep only GTFS records whose route_id exists
    in the static stop sequence data.
    """

    valid_routes = stop_df.select("route_id").distinct()

    return gtfs_df.join(
        valid_routes,
        on="route_id",
        how="inner"
    )


def remove_duplicate_records(df):
    """
    Remove duplicate GTFS records.
    """

    return df.dropDuplicates()