from pyspark.sql import functions as F

def build_historical_table(travel_df):

    historical_df = (
        travel_df
        .groupBy(
            "route_id",
            "from_sequence",
            "to_sequence"
        )
        .agg(
            F.avg("link_travel_time").alias("avg_travel_time"),
            F.stddev("link_travel_time").alias("std_travel_time"),
            F.min("link_travel_time").alias("min_travel_time"),
            F.max("link_travel_time").alias("max_travel_time"),
            F.count("*").alias("sample_count")
        )
    )

    return historical_df