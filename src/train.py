from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor


def assemble_features(train_df):
    """
    Assemble all predictor columns into a single features vector.
    """

    feature_columns = [
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
        "sample_count"
    ]

    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features"
    )

    return assembler.transform(train_df), assembler


def train_random_forest(train_df):

    train_df, assembler = assemble_features(train_df)

    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="link_travel_time",
        predictionCol="prediction",
        numTrees=100,
        maxDepth=10,
        seed=42
    )

    model = rf.fit(train_df)

    return model, assembler