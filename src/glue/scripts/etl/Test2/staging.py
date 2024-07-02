print("Executing staging.py")

from utils import load_config
import os
import logging

def get_dynamic_schema():
    print("Inside get_dynamic_schema")
    # Use absolute path based on current working directory
    config_path = os.path.abspath("./config.json")

    logging.info(f"Checking if config file exists at: {config_path}")
    print(f"Checking if config file exists at: {config_path}")
    
    if not os.path.exists(config_path):
        logging.error(f"Config file {config_path} not found.")
        print(f"Config file {config_path} not found.")
        raise FileNotFoundError(f"Config file {config_path} not found.")
    
    config = load_config(config_path)
    print(f"Config loaded: {config}")
    """
    Fetch the schema dynamically from Glue catalog.
    """
    catalog = glue_context.extract_jdbc_catalog()
    table_schema = catalog[database_name][table_name].schema()
    return table_schema

def stage_data(spark, glue_context, raw_s3_path, staging_s3_path, source, table_name, process_type):
    """
    Stage data by reading from the raw layer and writing to the staging layer in Parquet format with partitions.
    """
    config = load_config('config.json')
    raw_data_path = os.path.join(raw_s3_path, source, table_name, process_type)
    staging_data_path = os.path.join(staging_s3_path, source, table_name, process_type)
    print(f"Raw data path {raw_data_path}.")
    print(f"Staging data path {staging_data_path}.")

    database_name = config['glue_database']
    schema = get_dynamic_schema(glue_context, database_name, table_name)
    
    df = spark.read.schema(schema).csv(raw_data_path, header=True)

    partition_cols = config["tables"][table_name].get("partition_columns", [])
    date_column = config["tables"][table_name].get("date_column", None)
    
    if date_column:
        df = df.withColumn('year', year(df[date_column])) \
               .withColumn('month', month(df[date_column]))
    
    df.write.partitionBy(partition_cols).parquet(staging_data_path, mode="overwrite")
    print(f"Data staged from {raw_data_path} to {staging_data_path}.")

if __name__ == "__main__":
    # These paths should come from your configuration or environment variables
    hadoop_aws_jar = "/home/glue_user/jars/hadoop-aws-3.2.0.jar"
    aws_sdk_jar = "/home/glue_user/jars/aws-java-sdk-bundle-1.11.375.jar"
    raw_s3_path = "s3a://ecommerce-data-lake-730335322582-us-east-1-dev/01_raw/"
    staging_s3_path = "s3a://ecommerce-data-lake-730335322582-us-east-1-dev/02_staging/"
    source = "netsuite"
    table_name = "orders"
    process_type = "full_load"
    
    spark = get_spark_session(hadoop_aws_jar, aws_sdk_jar)
    glue_context = get_glue_context()
    config = load_config('config.json')
    stage_data(spark, glue_context, raw_s3_path, staging_s3_path, source, table_name, process_type, config)
