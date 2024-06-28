import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_session(hadoopaws_jar_path: str, awssdk_jar_path: str) -> SparkSession:
    """
    Initialize and return a Spark session.
    """
    try:
        spark = SparkSession.builder \
            .config("spark.jars", f"{hadoopaws_jar_path},{awssdk_jar_path}") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
            .config("spark.driver.extraJavaOptions", "--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED") \
            .getOrCreate()
        logger.info(f"Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

def read_config(config_path: str) -> dict:
    """
    Read and return the JSON configuration file.
    """
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info(f"Configuration file '{config_path}' read successfully.")
        return config
    except Exception as e:
        logger.error(f"Failed to read configuration file '{config_path}': {e}")
        raise

def upload_to_s3(file_path: str, bucket_name: str, s3_key: str):
    """
    Upload a file to S3.
    """
    try:
        s3 = boto3.client('s3')
        s3.upload_file(file_path, bucket_name, s3_key)
        logger.info(f"File '{file_path}' uploaded to S3 bucket '{bucket_name}' with key '{s3_key}'.")
    except Exception as e:
        logger.error(f"Failed to upload file to S3: {e}")
        raise

def validate_data(df, checks):
    """
    Validate data using PyDeequ and return the result.
    """
    try:
        result = VerificationSuite(df.sparkSession).onData(df).addCheck(checks).run()
        logger.info(f"Data validation completed with status: {result.status}")
        return VerificationResult.successMetricsAsDataFrame(df.sparkSession, result)
    except Exception as e:
        logger.error(f"Data validation failed: {e}")
        raise

def add_ingestion_date(df):
    """
    Add an ingestion date column to the DataFrame.
    """
    try:
        df = df.withColumn("ingestion_date", current_date())
        logger.info("Ingestion date added to DataFrame.")
        return df
    except Exception as e:
        logger.error(f"Failed to add ingestion date: {e}")
        raise

def remove_duplicates(df, columns):
    """
    Remove duplicates from the DataFrame based on the specified columns.
    """
    try:
        df = df.dropDuplicates(columns)
        logger.info(f"Duplicates removed from DataFrame based on columns: {columns}.")
        return df
    except Exception as e:
        logger.error(f"Failed to remove duplicates: {e}")
        raise

def log_etl_step(step_name: str, status: str, log_bucket: str, log_key: str):
    """
    Log the ETL step status to S3.
    """
    log_entry = {
        "step_name": step_name,
        "status": status,
        "timestamp": datetime.now().isoformat()
    }
    try:
        s3 = boto3.client('s3')
        s3.put_object(Bucket=log_bucket, Key=log_key, Body=json.dumps(log_entry))
        logger.info(f"ETL step '{step_name}' logged with status '{status}'.")
    except Exception as e:
        logger.error(f"Failed to log ETL step to S3: {e}")
        raise

def load_iceberg_table(spark, table_name: str, path: str):
    """
    Load data from an Iceberg table.
    """
    try:
        df = spark.read.format("iceberg").load(path + table_name)
        logger.info(f"Iceberg table '{table_name}' loaded from path '{path}'.")
        return df
    except Exception as e:
        logger.error(f"Failed to load Iceberg table '{table_name}': {e}")
        raise

def save_iceberg_table(df, table_name: str, path: str):
    """
    Save data to an Iceberg table.
    """
    try:
        df.write.format("iceberg").mode("overwrite").save(path + table_name)
        logger.info(f"Data saved to Iceberg table '{table_name}' at path '{path}'.")
    except Exception as e:
        logger.error(f"Failed to save data to Iceberg table '{table_name}': {e}")
        raise

def load_parquet_table(spark, path: str):
    """
    Load data from a Parquet table.
    """
    try:
        df = spark.read.parquet(path)
        logger.info(f"Parquet table loaded from path '{path}'.")
        return df
    except Exception as e:
        logger.error(f"Failed to load Parquet table: {e}")
        raise

def save_parquet_table(df, path: str):
    """
    Save data to a Parquet table.
    """
    try:
        df.write.mode("overwrite").parquet(path)
        logger.info(f"Data saved to Parquet table at path '{path}'.")
    except Exception as e:
        logger.error(f"Failed to save data to Parquet table: {e}")
        raise
