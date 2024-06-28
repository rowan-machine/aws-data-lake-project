import logging
from pyspark.sql import SparkSession

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def master_data(spark: SparkSession, preprocessed_s3_path: str, master_s3_path: str, table_name: str):
    logger.info(f"Mastering data for table: {table_name}")
    try:
        # Read preprocessed data
        preprocessed_data = spark.read.parquet(f"{preprocessed_s3_path}{table_name}/")
        logger.info("Preprocessed data read successfully")

        # Example transformation for mastering
        mastered_data = preprocessed_data.dropDuplicates(["unique_column"])

        # Write to master area
        mastered_data.write.mode("overwrite").parquet(f"{master_s3_path}{table_name}/")
        logger.info("Data written to master area successfully")
    except Exception as e:
        logger.error(f"Error mastering data for table {table_name}: {e}")
        raise

if __name__ == "__main__":
    # Example usage (this would be in your main script or test script)
    spark = SparkSession.builder.appName("Mastering").getOrCreate()
    master_data(spark, "s3a://preprocessed-data/", "s3a://master-data/", "orders")
