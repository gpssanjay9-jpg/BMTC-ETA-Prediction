from pyspark.sql import functions as F
from pyspark.sql.window import Window


def reconstruct_arrivals(arrival_df):
    """
    Fill missing stop arrivals using linear interpolation.

    Output:
    trip_id
    route_id
    sequence
    stop_id
    stop_name
    arrival_time
    is_interpolated
    """

    window = (
        Window
        .partitionBy("trip_id")
        .orderBy("sequence")
    )

    df = (
        arrival_df
        .withColumn(
            "next_sequence",
            F.lead("sequence").over(window)
        )
        .withColumn(
            "next_arrival",
            F.lead("arrival_time").over(window)
        )
    )

    gaps = (
        df
        .filter(
            F.col("next_sequence") > F.col("sequence") + 1
        )
        .withColumn(
            "missing_sequence",
            F.explode(
                F.sequence(
                    F.col("sequence") + 1,
                    F.col("next_sequence") - 1
                )
            )
        )
        .withColumn(
            "step",
            (
                F.col("next_arrival") -
                F.col("arrival_time")
            ) /
            (
                F.col("next_sequence") -
                F.col("sequence")
            )
        )
        .withColumn(
            "arrival_time",
            F.col("arrival_time") +
            (
                F.col("missing_sequence") -
                F.col("sequence")
            ) *
            F.col("step")
        )
        .withColumnRenamed(
            "missing_sequence",
            "sequence"
        )
        .withColumn(
            "is_interpolated",
            F.lit(True)
        )
        .select(
            "trip_id",
            "route_id",
            "sequence",
            "arrival_time",
            "is_interpolated"
        )
    )

    observed = (
        arrival_df
        .withColumn(
            "is_interpolated",
            F.lit(False)
        )
    )

    final = (
        observed
        .unionByName(
            gaps,
            allowMissingColumns=True
        )
        .orderBy(
            "trip_id",
            "sequence"
        )
    )

    return final