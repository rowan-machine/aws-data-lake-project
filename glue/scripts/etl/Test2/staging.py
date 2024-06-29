from pyspark.sql import SparkSession
import os
import utils

config = utils.load_config('config.json')

def stage_data(spark, raw_s3_path, staging_s3_path, source, table_name, process_type, partition_columns):
    """
    Stage data by reading from the raw layer and writing to the staging layer in Parquet format with partitions.
    """
    raw_data_path = os.path.join(raw_s3_path, source, table_name, process_type)
    staging_data_path = os.path.join(staging_s3_path, source, table_name, process_type)
    print(f"Raw data path {raw_data_path}.")
    print(f"Staging data path {staging_data_path}.")
    
    df = spark.read.csv(raw_data_path, header=True, inferSchema=True)

    partition_cols = config["tables"][table_name].get("partition_columns", [])
    date_column = config["tables"][table_name].get("date_column", None)
    
    if date_column:
        df = df.withColumn('year', year(df[date_column])) \
               .withColumn('month', month(df[date_column]))
    
    df.write.partitionBy(partition_cols).parquet(staging_data_path, mode="overwrite")
    print(f"Data staged from {raw_data_path} to {staging_data_path}.")

if __name__ == "__main__":
    # These paths should come from your configuration or environment variables
    hadoop_aws_jar = "/opt/glue/jars/hadoop-aws-3.2.0.jar"
    aws_sdk_jar = "/opt/glue/jars/aws-java-sdk-bundle-1.11.375.jar"
    raw_s3_path = "s3a://ecommerce-data-lake-730335322582-us-east-1-dev/01_raw/"
    staging_s3_path = "s3a://ecommerce-data-lake-730335322582-us-east-1-dev/02_staging/"
    source = "netsuite"
    table_name = "orders"
    process_type = "full_load"
    partition_columns = ["year", "month"]

    spark = utils.get_spark_session(hadoop_aws_jar, aws_sdk_jar)
    config = utils.load_config()
    stage_data(spark, raw_s3_path, staging_s3_path, source, table_name, process_type, config)
