from utils import save_iceberg_table

def aggregate_data(spark, df, args, paths):
    # Example aggregation logic
    aggregated_df = df.groupBy("some_column").agg({"another_column": "sum"})
    save_iceberg_table(aggregated_df, f"curated_catalog.{args['TABLE_NAME']}_aggregated")
    return aggregated_df
