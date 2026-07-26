from pyspark.sql import functions as F


def clean_column(df, column_name, data_type):
    """
    Clean a column by:
    1. Removing double quotes.
    2. Trimming whitespace.
    3. Converting empty strings to NULL.
    4. Safely casting to the required datatype.
    """

    cleaned = F.regexp_replace(F.col(column_name), '"', "")
    cleaned = F.trim(cleaned)
    cleaned = F.when(cleaned == "", None).otherwise(cleaned)

    return df.withColumn(
        column_name,
        cleaned.cast(data_type)
    )


def standardize_gtfs(df):
    """
    Standardize GTFS realtime data.
    """

    integer_columns = [
        "trip_id",
        "stop_id",
        "route_id",
        "vehicle_timestamp",
        "start_date",
        "vehicle_id"
    ]

    double_columns = [
        "latitude",
        "longitude",
        "bearing"
    ]

    string_columns = [
        "id",
        "system_time",
        "current_status",
        "start_time",
        "schedule_relationship",
        "label"
    ]

    for col in integer_columns:
        df = clean_column(df, col, "int")

    for col in double_columns:
        df = clean_column(df, col, "double")

    for col in string_columns:
        df = clean_column(df, col, "string")

    return df


def standardize_stop_sequence(df):
    """
    Standardize static stop sequence data.
    """

    integer_columns = [
        "route_id",
        "stop_id",
        "sequence"
    ]

    double_columns = [
        "stop_lat",
        "stop_lon"
    ]

    string_columns = [
        "stop_name"
    ]

    for col in integer_columns:
        df = clean_column(df, col, "int")

    for col in double_columns:
        df = clean_column(df, col, "double")

    for col in string_columns:
        df = clean_column(df, col, "string")

    return df