import random
from pyspark.sql import functions as F

from standardize import (
    standardize_gtfs,
    standardize_stop_sequence
)

from filter import (
    filter_schedule,
    filter_valid_routes,
    remove_duplicate_records
)

from snap import snap_gtfs


# -----------------------
# Read Data
# -----------------------

gtfs_path = "abfss://bmtc@practicedb12.dfs.core.windows.net/source/"

# Get all parquet files
all_files = [
    file.path
    for file in dbutils.fs.ls(gtfs_path)
    if file.path.endswith(".parquet")
]

# Randomly select 7 files
random.seed(42)   # Optional: makes the selection reproducible
sample_files = random.sample(all_files, 1)

# Read only those files
df_gtfs = spark.read.parquet(*sample_files)
stop_path = "abfss://bmtc@practicedb12.dfs.core.windows.net/source/csv/"


df_stop = (
    spark.read
    .option("header", True)
    .csv(stop_path)
)


# -----------------------
# Standardize
# -----------------------

df_gtfs = standardize_gtfs(df_gtfs)

df_stop = standardize_stop_sequence(df_stop)


# -----------------------
# Filter
# -----------------------

df_gtfs = filter_schedule(df_gtfs)

df_gtfs = filter_valid_routes(
    df_gtfs,
    df_stop
)

df_gtfs = remove_duplicate_records(df_gtfs)


# -----------------------
# Snap GPS
# -----------------------

df_gtfs = snap_gtfs(
    df_gtfs,
    df_stop
)


# -----------------------
# Exploratory Checks
# -----------------------

duplicate_timestamp = (
    df_gtfs
    .groupBy(
        "trip_id",
        "vehicle_timestamp"
    )
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate timestamp combinations:")
duplicate_timestamp.show()


print("Total GTFS rows")
print(df_gtfs.count())


print("Total Routes")
print(df_gtfs.select("route_id").distinct().count())


print("Sample")
df_gtfs.show(10, truncate=False)