import logging
from pyspark.sql import SparkSession

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def stage_data(spark: SparkSession, raw_s3_path: str, staging_s3_path: str, table_name: str):
    logger.info(f"Staging data for table: {table_name}")
    try:
        # Read raw data
        raw_data = spark.read.format("csv").option("header", "true").load(f"{raw_s3_path}{table_name}/")
        logger.info("Raw data read successfully")

        # Write to staging area
        raw_data.write.mode("overwrite").parquet(f"{staging_s3_path}{table_name}/")
        logger.info("Data written to staging area successfully")
    except Exception as e:
        logger.error(f"Error staging data for table {table_name}: {e}")
        raise

if __name__ == "__main__":
    # Example usage (this would be in your main script or test script)
    spark = SparkSession.builder.appName("Staging").getOrCreate()
    stage_data(spark, "s3a://raw-data/", "s3a://staging-data/", "orders")
