import sys
from utils import read_config, get_spark_session
from staging import stage_data
from preprocessing import preprocess_data
from mastering import master_data
from aggregations import aggregate_data

def main(args):
    config = read_config('config.json')[args['ENV']]
    spark = get_spark_session("ETL Job", config['spark']['iceberg_jar_path'], config['spark']['pydeequ_jar_path'], config['paths']['master'])

    # Stage Data
    staged_df = stage_data(spark, args, config['paths'])

    # Preprocess Data
    preprocessed_df = preprocess_data(spark, staged_df, args, config['paths'])

    # Master Data
    master_df = master_data(spark, preprocessed_df, args, config['paths'])

    # Aggregate Data
    aggregate_data(spark, master_df, args, config['paths'])

if __name__ == "__main__":
    args = {
        'ENV': sys.argv[1],  # dev or prod
        'TABLE_NAME': sys.argv[2],  # orders, customers, or products
        'PROCESS_TYPE': sys.argv[3],  # full_load or cdc
        'SOURCE': sys.argv[4]  # e.g., netSuite
    }
    main(args)
