from pyspark.ml.evaluation import RegressionEvaluator


def predict(model, assembler, test_df):
    """
    Generate predictions.
    """

    test_df = assembler.transform(test_df)

    prediction_df = model.transform(test_df)

    return prediction_df


def evaluate(prediction_df):
    """
    Evaluate regression model.
    """

    metrics = {}

    rmse = RegressionEvaluator(
        labelCol="link_travel_time",
        predictionCol="prediction",
        metricName="rmse"
    )

    mae = RegressionEvaluator(
        labelCol="link_travel_time",
        predictionCol="prediction",
        metricName="mae"
    )

    r2 = RegressionEvaluator(
        labelCol="link_travel_time",
        predictionCol="prediction",
        metricName="r2"
    )

    metrics["RMSE"] = rmse.evaluate(prediction_df)
    metrics["MAE"] = mae.evaluate(prediction_df)
    metrics["R2"] = r2.evaluate(prediction_df)

    return metrics


def print_metrics(metrics):

    print("\n========== MODEL PERFORMANCE ==========")

    print(f"RMSE : {metrics['RMSE']:.2f}")

    print(f"MAE  : {metrics['MAE']:.2f}")

    print(f"R²   : {metrics['R2']:.4f}")