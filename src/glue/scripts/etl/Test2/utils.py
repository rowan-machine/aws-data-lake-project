import os
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import json
import logging

print("Executing utils.py")

def load_config(config_file: str):
    print(f"Loading config from {config_file}")
    if not os.path.exists(config_file):
        print(f"Config file {config_file} not found.")
        raise FileNotFoundError(f"Config file {config_file} not found.")
    print(f"Loading config file from {config_file}")
    with open(config_file, 'r') as file:
        json_file = json.load(file)
    print(f"Config file loaded successfully")
    return json_file

def get_spark_session(hadoop_aws_jar_path: str, aws_sdk_jar_path: str):
    """
    Initialize and return a Spark session.
    """
    # Verify the JAR paths
    logger.info(f"Checking JAR paths: {hadoop_aws_jar_path}, {aws_sdk_jar_path}")
    if not os.path.exists(hadoop_aws_jar_path):
        logger.error(f"Hadoop AWS JAR path does not exist: {hadoop_aws_jar_path}")
    if not os.path.exists(aws_sdk_jar_path):
        logger.error(f"AWS SDK JAR path does not exist: {aws_sdk_jar_path}")

    try:
        spark = SparkSession.builder \
            .config("spark.jars", f"{hadoop_aws_jar_path},{aws_sdk_jar_path}") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
            .config("spark.driver.extraJavaOptions", "--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED") \
            .getOrCreate()
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

def get_glue_context():
    """
    Get or create a Glue context.
    """
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    return glue_context
