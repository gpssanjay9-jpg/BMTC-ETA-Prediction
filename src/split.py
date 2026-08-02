from pyspark.sql import functions as F


def remove_duplicates(df):
    return df.dropDuplicates()


def split_by_trip(df, train_fraction=0.8, seed=42):
    trips = (
        df
        .select("trip_id")
        .distinct()
    )

    train_trips, test_trips = trips.randomSplit(
        [train_fraction, 1 - train_fraction],
        seed=seed
    )

    train_df = (
        df.join(
            train_trips,
            on="trip_id",
            how="inner"
        )
    )

    test_df = (
        df.join(
            test_trips,
            on="trip_id",
            how="inner"
        )
    )

    return train_df, test_df


def drop_metadata(df):
    return df.drop("trip_id")


def prepare_train_test(feature_df):
    df = remove_duplicates(
        feature_df
    )

    train_df, test_df = split_by_trip(
        df
    )

    train_df = drop_metadata(
        train_df
    )

    test_df = drop_metadata(
        test_df
    )

    return train_df, test_df