from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
import pyspark.sql.functions as F
import logging

# Configure logging
logging.basicConfig(filename='logs/modelling.log', level=logging.INFO, format='%(asctime)s - %(message)s')

def create_spark_session():
    return SparkSession.builder.appName("JiraMetricsModeling").getOrCreate()

def load_cleaned_data(spark: SparkSession, file_path: str) -> DataFrame:
    # Load the cleaned data from parquet
    df = spark.read.parquet(file_path)
    return df

def feature_engineering(df: DataFrame) -> DataFrame:
    # Extract date features
    df = df.withColumn("year", F.year("date")).withColumn("month", F.month("date"))

    # StringIndexer for categorical columns
    engineer_indexer = StringIndexer(inputCol="engineer", outputCol="engineer_index")
    initiative_indexer = StringIndexer(inputCol="initiative", outputCol="initiative_index")
    repo_name_indexer = StringIndexer(inputCol="repo_name", outputCol="repo_name_index")

    # OneHotEncoder for indexed columns
    engineer_encoder = OneHotEncoder(inputCol="engineer_index", outputCol="engineer_vec")
    initiative_encoder = OneHotEncoder(inputCol="initiative_index", outputCol="initiative_vec")
    repo_name_encoder = OneHotEncoder(inputCol="repo_name_index", outputCol="repo_name_vec")

    # VectorAssembler to combine feature columns
    assembler = VectorAssembler(inputCols=["num_slack_messages", "num_hours", "engineer_vec", "initiative_vec", "repo_name_vec", "year", "month"], outputCol="features")

    # Pipeline to streamline feature engineering
    pipeline = Pipeline(stages=[engineer_indexer, initiative_indexer, repo_name_indexer, engineer_encoder, initiative_encoder, repo_name_encoder, assembler])

    # Fit and transform the DataFrame
    model = pipeline.fit(df)
    df = model.transform(df)

    return df

def train_and_evaluate_model(df: DataFrame) -> Pipeline:
    # Filter out rows where 'completed' is not null
    train_df = df.filter(df.completed.isNotNull())

    # Ensure the 'completed' column is strictly binary
    train_df = train_df.withColumn("completed_str", F.col("completed").cast("string"))

    # Split the data into training and validation sets
    train_data, val_data = train_df.randomSplit([0.8, 0.2], seed=42)

    # StringIndexer for the label column
    label_indexer = StringIndexer(inputCol="completed_str", outputCol="label")

    # RandomForestClassifier
    rf = RandomForestClassifier(featuresCol="features", labelCol="label")

    # Use TrainValidationSplit for hyperparameter tuning
    param_grid = ParamGridBuilder() \
        .addGrid(rf.numTrees, [20, 50]) \
        .addGrid(rf.maxDepth, [5, 10]) \
        .build()

    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")

    tvs = TrainValidationSplit(estimator=Pipeline(stages=[label_indexer, rf]),
                               estimatorParamMaps=param_grid,
                               evaluator=evaluator,
                               trainRatio=0.8)

    # Train the model using TrainValidationSplit
    model = tvs.fit(train_data)

    # Evaluate the model
    predictions = model.transform(val_data)
    auc = evaluator.evaluate(predictions)
    logging.info(f"Validation AUC: {auc}")

    return model

def predict_completed(df: DataFrame, model: Pipeline) -> DataFrame:
    # Filter out rows where 'completed' is null
    test_df = df.filter(df.completed.isNull())

    # Make predictions
    predictions = model.transform(test_df)

    # Select the relevant columns
    predictions = predictions.select("jira_ticket_id", "prediction")

    return predictions

def integrate_predictions(df: DataFrame, predictions: DataFrame) -> DataFrame:
    # Rename prediction column to completed
    predictions = predictions.withColumnRenamed("prediction", "completed_predicted")

    # Join the predictions back to the main DataFrame
    df = df.join(predictions, on="jira_ticket_id", how="left")

    # Cast completed_predicted to boolean
    df = df.withColumn("completed_predicted", F.col("completed_predicted").cast("boolean"))

    # If completed is null, use the predicted value
    df = df.withColumn("completed", F.when(df.completed.isNull(), df.completed_predicted).otherwise(df.completed))

    # Drop the temporary prediction column
    df = df.drop("completed_predicted")

    return df

if __name__ == "__main__":
    spark = create_spark_session()
    df = load_cleaned_data(spark, "../data/cleaned_data.parquet")
    df = feature_engineering(df)

    # Train and evaluate the model
    model = train_and_evaluate_model(df)

    # Predict 'completed' for null values
    predictions = predict_completed(df, model)

    # Integrate the predictions back into the main DataFrame
    df = integrate_predictions(df, predictions)
    
    df = df.drop("year", "month", "engineer_index", "initiative_index", "repo_name_index", "engineer_vec", "initiative_vec", "repo_name_vec", "features")
    df.write.mode("overwrite").parquet("data/predicted_datav0.parquet")