from utils import get_spark_session, read_config, add_ingestion_date, remove_duplicates, save_parquet_table, log_etl_step
import logging

logger = logging.getLogger(__name__)

def stage_data(spark, args, paths):
    try:
        raw_path = f"{paths['raw']}{args['SOURCE']}/{args['TABLE_NAME']}/{args['PROCESS_TYPE']}/"
        df = spark.read.format("csv").option("header", "true").load(raw_path)
        df = add_ingestion_date(df)
        df = remove_duplicates(df, ["order_id"])
        save_parquet_table(df, f"{paths['staging']}{args['TABLE_NAME']}/{args['PROCESS_TYPE']}/")
        log_etl_step("stage_data", "success", paths['log_bucket'], f"staging/{args['TABLE_NAME']}/{args['PROCESS_TYPE']}/log.json")
        return df
    except Exception as e:
        logger.error(f"Staging data failed: {e}")
        log_etl_step("stage_data", "failure", paths['log_bucket'], f"staging/{args['TABLE_NAME']}/{args['PROCESS_TYPE']}/log.json")
        raise
