from utils import save_iceberg_table, log_etl_step
import logging

logger = logging.getLogger(__name__)

def aggregate_data(spark, df, args, paths):
    try:
        aggregated_df = df.groupBy("some_column").agg({"another_column": "sum"})
        save_iceberg_table(aggregated_df, f"curated_catalog.{args['TABLE_NAME']}_aggregated", paths['curated'])
        log_etl_step("aggregate_data", "success", paths['log_bucket'], f"aggregations/{args['TABLE_NAME']}/{args['PROCESS_TYPE']}/log.json")
        return aggregated_df
    except Exception as e:
        logger.error(f"Aggregating data failed: {e}")
        log_etl_step("aggregate_data", "failure", paths['log_bucket'], f"aggregations/{args['TABLE_NAME']}/{args['PROCESS_TYPE']}/log.json")
        raise
