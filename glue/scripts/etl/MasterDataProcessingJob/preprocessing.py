from utils import save_parquet_table, log_etl_step
import logging

logger = logging.getLogger(__name__)

def preprocess_data(spark, df, args, paths):
    try:
        df = df.withColumn("preprocessed_column", df["some_column"] * 2)
        save_parquet_table(df, f"{paths['preprocessed']}{args['TABLE_NAME']}/{args['PROCESS_TYPE']}/")
        log_etl_step("preprocess_data", "success", paths['log_bucket'], f"preprocessing/{args['TABLE_NAME']}/{args['PROCESS_TYPE']}/log.json")
        return df
    except Exception as e:
        logger.error(f"Preprocessing data failed: {e}")
        log_etl_step("preprocess_data", "failure", paths['log_bucket'], f"preprocessing/{args['TABLE_NAME']}/{args['PROCESS_TYPE']}/log.json")
        raise
