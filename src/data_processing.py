from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
import logging

# Configure logging
logging.basicConfig(filename='logs/data_processing.log', level=logging.INFO, format='%(asctime)s - %(message)s')

def create_spark_session():
    return SparkSession.builder.appName("JiraMetrics").getOrCreate()

def load_data(spark: SparkSession, file_path: str) -> DataFrame:
    # Define the expected schema
    schema = T.StructType([
        T.StructField("jira_ticket_id", T.IntegerType(), True),
        T.StructField("date", T.DateType(), True),
        T.StructField("completed", T.StringType(), True),  # Initially as StringType to handle "yes", "True", "None"
        T.StructField("num_slack_messages", T.IntegerType(), True),
        T.StructField("num_hours", T.FloatType(), True),
        T.StructField("engineer", T.StringType(), True),
        T.StructField("ticket_description", T.StringType(), True),
        T.StructField("KPIs", T.ArrayType(
            T.StructType([
                T.StructField("initiative", T.StringType(), True),
                T.StructField("new_revenue", T.FloatType(), True)
            ])
        ), True),
        T.StructField("lines_per_repo", T.ArrayType(
            T.MapType(T.StringType(), T.IntegerType())
        ), True)
    ])

    # Load the JSON data into a DataFrame
    df = spark.read.schema(schema).json(file_path)

    return df

def explode_nested_columns(df: DataFrame) -> DataFrame:
    # Explode KPIs column
    df = df.withColumn("KPIs_exploded", F.explode_outer("KPIs")) \
           .select("*", "KPIs_exploded.*").drop("KPIs", "KPIs_exploded")

    # Explode lines_per_repo column
    df = df.withColumn("lines_per_repo_exploded", F.explode_outer("lines_per_repo")) \
           .select("*", F.expr("map_keys(lines_per_repo_exploded)[0]").alias("repo_name"),
                   F.expr("map_values(lines_per_repo_exploded)[0]").alias("lines_added")) \
           .drop("lines_per_repo", "lines_per_repo_exploded")

    return df

def clean_data(df: DataFrame) -> DataFrame:
    # Impute missing dates with January 1, 2024
    df = df.withColumn("date", F.when(F.col("date").isNull(), F.lit("2024-01-01")).otherwise(F.col("date")))

    # Convert 'completed' field to BooleanType
    df = df.withColumn("completed", F.when(F.col("completed") == "yes", True)
                                      .when(F.col("completed") == "True", True)
                                      .when(F.col("completed") == "None", False)
                                      .otherwise(F.col("completed").cast(T.BooleanType())))

    # Replace nulls in 'engineer' with 'Unknown'
    df = df.withColumn("engineer", F.when(F.col("engineer").isNull(), "Unknown").otherwise(F.col("engineer")))

    # Impute missing 'num_slack_messages' with median
    median_slack_messages = df.approxQuantile("num_slack_messages", [0.5], 0.01)[0]
    df = df.withColumn("num_slack_messages", F.when(F.col("num_slack_messages").isNull(), median_slack_messages).otherwise(F.col("num_slack_messages")))

    # Impute missing 'num_hours' with median and correct negative values
    median_num_hours = df.approxQuantile("num_hours", [0.5], 0.01)[0]
    df = df.withColumn("num_hours", F.when((F.col("num_hours").isNull()) | (F.col("num_hours") < 0), median_num_hours).otherwise(F.col("num_hours")))

    # Handling missing or invalid 'ticket_description'
    df = df.withColumn("ticket_description", F.when(F.col("ticket_description").isNull(), "No Description").otherwise(F.col("ticket_description")))

    # Handling missing or invalid 'initiative' and 'new_revenue' in exploded KPIs
    df = df.withColumn("initiative", F.when(F.col("initiative").isNull(), "No Initiative").otherwise(F.col("initiative")))
    df = df.withColumn("new_revenue", F.when(F.col("new_revenue").isNull(), 0).otherwise(F.col("new_revenue")))

    # Handling missing or invalid 'repo_name' and 'lines_added' in exploded lines_per_repo
    df = df.withColumn("repo_name", F.when(F.col("repo_name").isNull(), "Unknown").otherwise(F.col("repo_name")))
    df = df.withColumn("lines_added", F.when(F.col("lines_added").isNull(), 0).otherwise(F.col("lines_added")))

    # Ensure unique jira_ticket_id
    df = df.dropDuplicates(["jira_ticket_id"])

    return df

if __name__ == "__main__":
    spark = create_spark_session()
    df = load_data(spark, "data/data.json")
    df = explode_nested_columns(df)
    df = clean_data(df)
    #save the data to parquet in the data folder
    df.write.mode("overwrite").parquet("data/cleaned_data.parquet")