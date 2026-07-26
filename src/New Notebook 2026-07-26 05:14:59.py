# Databricks notebook source
from pyspark.sql import functions as F
import sys

# Add project folder to Python path
sys.path.append("/Workspace/BMTC ETA Project")

# Import modules
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


# -----------------------------
# 1. Read GTFS realtime data
# -----------------------------

gtfs_path = "abfss://bmtc@practicedb12.dfs.core.windows.net/source/part-00000-00f11555-fd34-4104-ab5b-583b5d999cc2.c000.snappy_flat.parquet"

# Read one file for development
files = [
    f.path
    for f in dbutils.fs.ls(gtfs_path)
    if f.path.endswith(".parquet")
]

df_gtfs = spark.read.parquet(files[0])


# -----------------------------
# 2. Read static stop sequence
# -----------------------------

stop_path = "abfss://bmtc@practicedb12.dfs.core.windows.net/source/csv/"

df_stop = spark.read.csv(
    stop_path,
    header=True
)


# -----------------------------
# 3. Standardization
# -----------------------------

df_gtfs = standardize_gtfs(df_gtfs)

df_stop = standardize_stop_sequence(df_stop)


# -----------------------------
# 4. Filtering
# -----------------------------

df_gtfs = filter_schedule(df_gtfs)

df_gtfs = remove_duplicate_records(df_gtfs)

df_gtfs = filter_valid_routes(
    df_gtfs,
    df_stop
)


# -----------------------------
# 5. Snap GPS to stops
# -----------------------------

df_snapped = snap_gtfs(
    df_gtfs,
    df_stop
)


# -----------------------------
# 6. Check result
# -----------------------------

print("Final snapped rows:", df_snapped.count())

df_snapped.select(
    "trip_id",
    "vehicle_timestamp",
    "route_id",
    "sequence",
    "stop_name",
    "distance_to_stop"
).show(20, truncate=False)

# COMMAND ----------

import sys

sys.path.append("/Workspace/BMTC ETA Project")

from arrival import extract_arrivals

# COMMAND ----------

