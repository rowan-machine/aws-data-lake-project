from utils import save_iceberg_table, log_etl_step
import logging

logger = logging.getLogger(__name__)

def master_data(spark, df, args, paths):
    try:
        df = df.withColumn("mastered_column", df["preprocessed_column"] + 1)
        save_iceberg_table(df, f"master_catalog.{args['TABLE_NAME']}", paths['master'])
        log_etl_step("master_data", "success", paths['log_bucket'], f"mastering/{args['TABLE_NAME']}/{args['PROCESS_TYPE']}/log.json")
        return df
    except Exception as e:
        logger.error(f"Mastering data failed: {e}")
        log_etl_step("master_data", "failure", paths['log_bucket'], f"mastering/{args['TABLE_NAME']}/{args['PROCESS_TYPE']}/log.json")
        raise
