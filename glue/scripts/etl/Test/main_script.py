import staging
from utils import get_spark_session
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting the main function")

    # Parameters
    raw_s3_path = "s3a://ecommerce-data-lake-us-east-1-dev/01_raw/"
    staging_s3_path = "s3a://ecommerce-data-lake-us-east-1-dev/02_staging/"
    table_name = "orders"

    # Paths to JAR files
    hadoop_aws_jar_path = "/opt/glue/jars/hadoop-aws-3.2.0.jar"
    aws_sdk_jar_path = "/opt/glue/jars/aws-java-sdk-bundle-1.11.375.jar"

    try:
        # Create the Spark session
        logger.info("Creating Spark session")
        spark = get_spark_session(hadoop_aws_jar_path, aws_sdk_jar_path)
        logger.info("Spark session created successfully")
    except Exception as e:
        logger.error(f"Exception while creating Spark session: {e}")
        return

    # Stage data
    try:
        logger.info("Starting data staging")
        staging.stage_data(spark, raw_s3_path, staging_s3_path, table_name)
        logger.info("Data staging completed successfully")
    except Exception as e:
        logger.error(f"Error in staging data: {e}")

if __name__ == "__main__":
    main()
