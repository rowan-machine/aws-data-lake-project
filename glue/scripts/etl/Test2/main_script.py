import staging
import preprocessing
import mastering
import utils
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting the main function")

    # Parameters
    config = utils.load_config('config.yaml')
    raw_s3_path = config['paths']['raw_s3_path']
    staging_s3_path = config['paths']['staging_s3_path']
    preprocessed_s3_path = config['paths']['preprocessed_s3_path']
    master_s3_path = config['paths']['master_s3_path']
    table_name = config['table_name']
    process_type = config['process_type']

    # Paths to JAR files
    hadoop_aws_jar_path = config['jars']['hadoop_aws']
    aws_sdk_jar_path = config['jars']['aws_sdk']

    try:
        # Create the Spark session
        logger.info("Creating Spark session")
        spark = utils.get_spark_session(hadoop_aws_jar_path, aws_sdk_jar_path)
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

    # Preprocess data
    try:
        logger.info("Starting data preprocessing")
        preprocessing.preprocess_data(spark, staging_s3_path, preprocessed_s3_path, table_name, process_type)
        logger.info("Data preprocessing completed successfully")
    except Exception as e:
        logger.error(f"Error in preprocessing data: {e}")

    # Master data
    try:
        logger.info("Starting data mastering")
        mastering.master_data(spark, preprocessed_s3_path, master_s3_path, table_name)
        logger.info("Data mastering completed successfully")
    except Exception as e:
        logger.error(f"Error in mastering data: {e}")

if __name__ == "__main__":
    main()
