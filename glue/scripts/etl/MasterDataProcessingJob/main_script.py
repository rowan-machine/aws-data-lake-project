import sys
from utils import read_config, get_spark_session, log_etl_step
from staging import stage_data
from preprocessing import preprocess_data
from mastering import master_data
from aggregations import aggregate_data
import logging

logger = logging.getLogger(__name__)

def main(args):
    try:
        config = read_config('config.json')[args['ENV']]
        spark = get_spark_session("MasterDataProcessingJob", config['spark']['iceberg_jar_path'], config['spark']['pydeequ_jar_path'], config['spark']['hadoopaws_jar_path'], config['spark']['awssdk_jar_path'], config['paths']['master'])

        # Stage Data
        staged_df = stage_data(spark, args, config['paths'])

        # Preprocess Data
        preprocessed_df = preprocess_data(spark, staged_df, args, config['paths'])

        # Validate Preprocessed Data
        validate_data(preprocessed_df, "preprocessed", config['paths'])

        # Master Data
        master_df = master_data(spark, preprocessed_df, args, config['paths'])

        # Validate Master Data
        validate_data(master_df, "master", config['paths'])

        # Aggregate Data
        aggregated_df = aggregate_data(spark, master_df, args, config['paths'])

        # Validate Aggregated Data
        validate_data(aggregated_df, "aggregated", config['paths'])

        log_etl_step("main_script", "success", config['paths']['log_bucket'], "main_script/log.json")
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        log_etl_step("main_script", "failure", config['paths']['log_bucket'], "main_script/log.json")
        raise

if __name__ == "__main__":
    args = {
        'ENV': sys.argv[1],  # dev or prod
        'TABLE_NAME': sys.argv[2],  # orders, customers, or products
        'PROCESS_TYPE': sys.argv[3],  # full_load or cdc
        'SOURCE': sys.argv[4],  # e.g., netSuite
        'MASTER_S3_PATH': sys.argv[5]  # e.g., s3://path-to-master-data
    }
    main(args)
