import logging
import os
import sys
import subprocess

# Define parameters in a dictionary
parameters = {
    'JOB_NAME': 'FullLoadAndCDCProcesserJob',
    'RAW_S3_PATH': 's3://ecommerce-data-lake-us-east-1-dev/01_raw/',
    'STAGING_S3_PATH': 's3://ecommerce-data-lake-us-east-1-dev/02_staging/',
    'PREPROCESSED_S3_PATH': 's3://ecommerce-data-lake-us-east-1-dev/03_preprocessed/',
    'MASTER_S3_PATH': 's3://ecommerce-data-lake-us-east-1-dev/04_master/',
    'CURATED_S3_PATH': 's3://ecommerce-data-lake-us-east-1-dev/06_curated/',
    'TABLE_NAME': 'orders',
    'PROCESS_TYPE': 'full_load',
    'SOURCE': 'netSuite'
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/var/log/glue/glue_script.log")
    ]
)
logger = logging.getLogger(__name__)

# Install Deequ 
subprocess.check_call([sys.executable, "-m", "pip", "install", "--target=/tmp", "pydeequ"])
sys.path.insert(0, '/tmp')

# Explicitly set SPARK_VERSION if not already set 
if 'SPARK_VERSION' not in os.environ:
    os.environ['SPARK_VERSION'] = '3.1'

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, expr, when
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'RAW_S3_PATH', 
    'STAGING_S3_PATH', 
    'PREPROCESSED_S3_PATH', 
    'MASTER_S3_PATH',
    'CURATED_S3_PATH', 
    'TABLE_NAME', 
    'PROCESS_TYPE',
    'SOURCE'
])

# Path to the Iceberg JAR file
iceberg_jar_path = "/opt/glue/jars/iceberg-spark-3.1_2.12-1.3.1.jar"
hadoop_aws_jar_path = "/opt/glue/jars/hadoop-aws-3.2.0.jar"
aws_sdk_jar_path = "/opt/glue/jars/aws-java-sdk-bundle-1.11.375.jar"

try:
    # Initialize Spark session with Iceberg and S3 configurations
    spark = SparkSession.builder \
        .config("spark.jars", ",".join([iceberg_jar_path, hadoop_aws_jar_path, aws_sdk_jar_path])) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.master_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.master_catalog.type", "hadoop") \
        .config("spark.sql.catalog.master_catalog.warehouse", args['MASTER_S3_PATH']) \
        .config("spark.sql.catalog.curated_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.curated_catalog.type", "hadoop") \
        .config("spark.sql.catalog.curated_catalog.warehouse", args['CURATED_S3_PATH']) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

    glueContext = GlueContext(SparkContext.getOrCreate())
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)


    logger.info("Spark session and Glue context initialized successfully")

    # Your data processing logic here
    logger.info("Starting data processing...")

    # Read source data
    source_df = spark.read.format("csv").option("header", "true").load(args['RAW_S3_PATH'] + args['SOURCE'] + '/' + args['TABLE_NAME'] + '/' + args['PROCESS_TYPE'])
    logger.info("Source data loaded successfully")


    # Perform transformations, validations, and save data as Parquet
    
    # Data Quality Checks

    logger.info("Data processing completed successfully")

except Exception as e:
    logger.error("Error in Glue job: %s", str(e))
    raise

logger.info("Glue job script execution completed")


# Commit job
job.commit()
