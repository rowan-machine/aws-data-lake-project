import staging
from utils import get_spark_session, load_config
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("Executing main_script.py")

def main():
    print("Inside main function")
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Starting the main function")
    print("Starting the main function")

    # Use absolute path based on current working directory
    config_path = os.path.abspath("./config.json")

    logger.info(f"Checking if config file exists at: {config_path}")
    print(f"Checking if config file exists at: {config_path}")
    
    if not os.path.exists(config_path):
        logger.error(f"Config file {config_path} not found.")
        print(f"Config file {config_path} not found.")
        return
    
    config = utils.load_config(config_path)
    logger.info(f"Config loaded: {config}")
    print(f"Config loaded: {config}")

    # Parameters
    raw_s3_path = config['paths']['raw_s3_path']
    staging_s3_path = config['paths']['staging_s3_path']
    preprocessed_s3_path = config['paths']['preprocessed_s3_path']
    master_s3_path = config['paths']['master_s3_path']
    source = config['common']['source']
    table_name = config['common']['table_name']
    process_type = config['common']['process_type']

    # Paths to JAR files
    hadoop_aws_jar_path = config['jars']['hadoop_aws']
    aws_sdk_jar_path = config['jars']['aws_sdk']

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
        staging.stage_data(spark, raw_s3_path, staging_s3_path, source, table_name, process_type)
        logger.info("Data staging completed successfully")
    except Exception as e:
        logger.error(f"Error in staging data: {e}")

if __name__ == "__main__":
    main()
