import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import logging
from dotenv import load_dotenv  # Add this import
import os  # Ensure os is imported
load_dotenv()  # Load environment variables from .env file
credentials_path='../credentials.json'

def parse_billing_period(project_id: str, dataset: str, input_table: str, output_table: str):
    spark = SparkSession.builder \
        .appName("ParseBillingPeriod") \
        .config("spark.sql.catalog.spark_bigquery", "com.google.cloud.spark.bigquery.BigQueryCatalog") \
        .config("parentProject", project_id)\
        .getOrCreate()

    # Load the Subscription Payments table from BigQuery
    df = spark.read \
        .format("bigquery") \
        .option("dataset", f"{dataset}")\
        .option("table", f"{input_table}") \
        .option("credentialsFile", credentials_path)\
        .load()

    # Extract start and end dates from billingPeriod
    df_parsed = df.withColumn(
        "start_date", expr("substring(billing_period, 1, 8)")  # Extracts YYYYMMDD
    ).withColumn(
        "end_date", expr("substring(billing_period, 10, 8)")    # Extracts YYYYMMDD
    )

    # Convert to date format (optional, if needed)
    df_parsed = df_parsed.withColumn("start_date", expr("to_date(start_date, 'yyyyMMdd')")) \
                         .withColumn("end_date", expr("to_date(end_date, 'yyyyMMdd')"))

    # Select relevant columns
    df_result = df_parsed.select("*")  # Adjust as necessary
    print(df_result.show())
    print(df_result.printSchema())

    # Write the result back to BigQuery
    df_result.write \
        .format("bigquery") \
        .mode("overwrite") \
        .option("writeMethod", "direct") \
        .option("table", f"{project_id}.{dataset}.{output_table}") \
        .option("credentialsFile", credentials_path)\
        .save()
    logging.info(f"Successful saving to BigQuery")

    spark.stop()

if __name__ == "__main__":
    # Accessing project_id and dataset_name using dlt.secrets
    project_id = os.getenv('PROJECT_ID')  # Update to use environment variable
    dataset = os.getenv('DATASET')  # Update to use environment variable
    input_table = os.getenv('INPUT_TABLE')  # Update to use environment variable
    output_table = os.getenv('OUTPUT_TABLE')  # Update to use environment variable
    parse_billing_period(project_id, dataset, input_table, output_table)