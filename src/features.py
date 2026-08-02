from pyspark.sql import functions as F


def extract_temporal_features(df):
    """
    Extract temporal features from arrival_time.
    """

    return (
        df
        .withColumn("hour", F.hour("arrival_time"))
        .withColumn("weekday", F.dayofweek("arrival_time"))
        .withColumn(
            "is_weekend",
            F.when(
                F.col("weekday").isin(1, 7),
                1
            ).otherwise(0)
        )
    )


def join_historical(travel_df, historical_df):
    """
    Join historical statistics to every observed link.
    """

    return (
        travel_df.join(
            historical_df,
            on=[
                "route_id",
                "from_sequence",
                "to_sequence"
            ],
            how="left"
        )
    )


def clean_features(df):
    """
    Replace NULL feature values.
    """

    return (
        df.fillna(
            {
                "std_travel_time": 0
            }
        )
    )


def select_features(df):
    """
    Select final feature columns.
    trip_id is retained only for train/test splitting.
    """

    return (
        df.select(
            "trip_id",
            "route_id",
            "from_sequence",
            "to_sequence",
            "hour",
            "weekday",
            "is_weekend",
            "distance_to_stop",
            "avg_travel_time",
            "std_travel_time",
            "min_travel_time",
            "max_travel_time",
            "sample_count",
            "link_travel_time"
        )
    )


def build_feature_table(travel_df, historical_df):
    """
    Complete feature engineering pipeline.
    """

    df = extract_temporal_features(
        travel_df
    )

    df = join_historical(
        df,
        historical_df
    )

    df = clean_features(
        df
    )

    df = select_features(
        df
    )

    return df