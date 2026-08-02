from pyspark.sql import functions as F


def check_schema(df):
    """
    Print schema.
    """

    print("\n========== SCHEMA ==========")
    df.printSchema()


def check_row_count(df):
    """
    Print total number of rows.
    """

    print("\n========== ROW COUNT ==========")
    print(f"Rows : {df.count()}")


def check_nulls(df):
    """
    Count NULL values in every column.
    """

    print("\n========== NULL VALUES ==========")

    df.select([
        F.count(
            F.when(F.col(c).isNull(), c)
        ).alias(c)
        for c in df.columns
    ]).show(truncate=False)


def check_duplicates(df):
    """
    Compare total rows with unique rows.
    """

    print("\n========== DUPLICATES ==========")

    total = df.count()
    unique = df.dropDuplicates().count()

    print(f"Total Rows  : {total}")
    print(f"Unique Rows : {unique}")
    print(f"Duplicates  : {total - unique}")


def check_target(df):
    """
    Summary statistics of target variable.
    """

    print("\n========== TARGET ==========")

    df.select(
        "link_travel_time"
    ).describe().show()


def check_sample_count(df):
    """
    Distribution of historical sample counts.
    """

    print("\n========== SAMPLE COUNT ==========")

    (
        df
        .groupBy("sample_count")
        .count()
        .orderBy("sample_count")
        .show(20, False)
    )


def check_distance(df):
    """
    Summary of snapping distance.
    """

    print("\n========== DISTANCE TO STOP ==========")

    df.select(
        "distance_to_stop"
    ).describe().show()


def validate_features(df):
    """
    Complete validation pipeline.
    """

    check_schema(df)

    check_row_count(df)

    check_nulls(df)

    check_duplicates(df)

    check_target(df)

    check_sample_count(df)

    check_distance(df)