from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging

# Configure logging
logging.basicConfig(filename='logs/final_analysis.log', level=logging.INFO, format='%(asctime)s - %(message)s')

def create_spark_session():
    return SparkSession.builder.appName("JiraMetricsAnalysis").getOrCreate()

def load_cleaned_data(spark: SparkSession, file_path: str) -> DataFrame:
    return spark.read.parquet(file_path)

def longest_ticket_description(df: DataFrame) -> str:
    description = df.orderBy(F.length("ticket_description").desc()).select("ticket_description").limit(1).collect()[0][0]
    return description

def repo_with_most_lines(df: DataFrame) -> str:
    repo = df.groupBy("repo_name").agg(F.sum("lines_added").alias("total_lines")).orderBy(F.desc("total_lines")).limit(1).select("repo_name").collect()[0][0]
    return repo

def max_slack_messages_per_engineer(df: DataFrame) -> DataFrame:
    # Group by engineer and find the maximum number of Slack messages in any ticket for each engineer
    return df.groupBy("engineer").agg(F.max("num_slack_messages").alias("max_messages_per_ticket")).orderBy("engineer")

def mean_hours_june_2023(df: DataFrame) -> float:
    mean_hours = df.filter((F.year("date") == 2023) & (F.month("date") == 6)).agg(F.mean("num_hours").alias("mean_hours")).collect()[0][0]
    return mean_hours

def total_lines_completed_repo_a(df: DataFrame) -> int:
    total_lines = df.filter((F.col("completed") == True) & (F.col("repo_name") == "A")).agg(F.sum("lines_added").alias("total_lines")).collect()[0][0]
    return total_lines

def total_revenue_per_engineer_initiative(df: DataFrame) -> DataFrame:
    result = df.groupBy("engineer", "initiative").agg(F.sum("new_revenue").alias("total_revenue")).orderBy("engineer", "initiative")
    result = result.withColumn("total_revenue", F.format_number("total_revenue", 2))
    return result

if __name__ == "__main__":
    spark = create_spark_session()
    df = load_cleaned_data(spark, "data/cleaned_data.parquet")

    # Q1: What is the longest Jira ticket description?
    result_q1 = longest_ticket_description(df)
    logging.info(f"Longest ticket description is: {result_q1}")
    logging.info("****************************************************************************************")

    # Q2: Which repo has the most lines of code added?
    result_q2 = repo_with_most_lines(df)
    logging.info(f"Repo with the most lines of code added is: {result_q2}")
    logging.info("****************************************************************************************")

    # Q3: Provide the maximum number of Slack messages in any ticket for each engineer
    result_q3 = max_slack_messages_per_engineer(df)
    logging.info("Maximum number of Slack messages in any ticket for each engineer:")
    logging.info(result_q3.show())
    logging.info("****************************************************************************************")

    # Q4: Mean hours spent on a ticket in June 2023
    result_q4 = mean_hours_june_2023(df)
    logging.info(f"Mean hours spent on a ticket in June 2023: {result_q4}")
    logging.info("****************************************************************************************")

    # Q5: Total lines of code contributed by completed tickets to the repo 'A'
    result_q5 = total_lines_completed_repo_a(df)
    logging.info(f"Total lines of code contributed by completed tickets to the repo 'A': {result_q5}")
    logging.info("****************************************************************************************")

    # Q6: Total new revenue per engineer per company initiative
    result_q6 = total_revenue_per_engineer_initiative(df)
    logging.info("Total new revenue per engineer per company initiative:")
    logging.info(result_q6.show())