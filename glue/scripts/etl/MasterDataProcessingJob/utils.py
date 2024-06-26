import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pydeequ.verification import VerificationSuite, VerificationResult
import json

def get_spark_session(app_name: str, iceberg_jar_path: str, pydeequ_jar_path: str, warehouse_path: str) -> SparkSession:
    """
    Initialize and return a Spark session with Iceberg and PyDeequ configurations.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", f"{iceberg_jar_path},{pydeequ_jar_path}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.master_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.master_catalog.type", "hadoop") \
        .config("spark.sql.catalog.master_catalog.warehouse", warehouse_path) \
        .getOrCreate()

def read_config(config_path: str) -> dict:
    """
    Read and return the JSON configuration file.
    """
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config

def upload_to_s3(file_path: str, bucket_name: str, s3_key: str):
    """
    Upload a file to S3.
    """
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket_name, s3_key)

def validate_data(df, checks):
    """
    Validate data using PyDeequ and return the result.
    """
    result = VerificationSuite(df.sparkSession).onData(df).addCheck(checks).run()
    return VerificationResult.successMetricsAsDataFrame(df.sparkSession, result)

def add_ingestion_date(df):
    """
    Add an ingestion date column to the DataFrame.
    """
    return df.withColumn("ingestion_date", current_date())

def remove_duplicates(df, columns):
    """
    Remove duplicates from the DataFrame based on the specified columns.
    """
    return df.dropDuplicates(columns)

def log_etl_step(step_name: str, status: str, log_bucket: str, log_key: str):
    """
    Log the ETL step status to S3.
    """
    log_entry = {
        "step_name": step_name,
        "status": status,
        "timestamp": datetime.now().isoformat()
    }
    s3 = boto3.client('s3')
    s3.put_object(Bucket=log_bucket, Key=log_key, Body=json.dumps(log_entry))

def load_iceberg_table(spark, table_name: str, path: str):
    """
    Load data from an Iceberg table.
    """
    return spark.read.format("iceberg").load(path + table_name)

def save_iceberg_table(df, table_name: str, path: str):
    """
    Save data to an Iceberg table.
    """
    df.write.format("iceberg").mode("overwrite").save(path + table_name)

def load_parquet_table(spark, path: str):
    """
    Load data from a Parquet table.
    """
    return spark.read.parquet(path)

def save_parquet_table(df, path: str):
    """
    Save data to a Parquet table.
    """
    df.write.mode("overwrite").parquet(path)
