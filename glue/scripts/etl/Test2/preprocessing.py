from pyspark.sql import SparkSession
from utils import get_spark_session

def preprocess_data(spark, staging_s3_path, preprocessed_s3_path, table_name, process_type):
    """
    Preprocess data from the staging layer and write to the preprocessed layer.
    """
    staging_data_path = os.path.join(staging_s3_path, table_name)
    preprocessed_data_path = os.path.join(preprocessed_s3_path, table_name, process_type)
    
    df = spark.read.parquet(staging_data_path)
    
    # Example transformation (you should add your own logic here)
    df = df.withColumn("total_amount", df["quantity"] * df["price"])
    
    df.write.parquet(preprocessed_data_path, mode="overwrite")
    print(f"Data preprocessed from {staging_data_path} to {preprocessed_data_path}.")

if __name__ == "__main__":
    # These paths should come from your configuration or environment variables
    hadoop_aws_jar = "/opt/glue/jars/hadoop-aws-3.2.0.jar"
    aws_sdk_jar = "/opt/glue/jars/aws-java-sdk-bundle-1.11.375.jar"
    staging_s3_path = "s3a://ecommerce-data-lake-730335322582-us-east-1-dev/02_staging/"
    preprocessed_s3_path = "s3a://ecommerce-data-lake-730335322582-us-east-1-dev/03_preprocessed/"
    table_name = "orders"
    process_type = "full_load"

    spark = get_spark_session(hadoop_aws_jar, aws_sdk_jar)
    preprocess_data(spark, staging_s3_path, preprocessed_s3_path, table_name, process_type)
