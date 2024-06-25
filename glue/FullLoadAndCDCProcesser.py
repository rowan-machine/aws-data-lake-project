import sys
import subprocess

# Install Deequ
subprocess.check_call([sys.executable, "-m", "pip", "install", "--target=/tmp", "pydeequ"])
sys.path.insert(0, '/tmp')

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, expr, when
from pyspark.sql.window import Window
import pyspark.sql.functions as F

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'RAW_S3_PATH', 
    'STAGING_S3_PATH', 
    'PREPROCESSED_S3_PATH', 
    'MASTER_S3_PATH',
    'CURATED_S3_PATH', 
    'TABLE_NAME', 
    'PROCESS_TYPE',
    'SOURCE'
])

# Initialize Spark session with Iceberg configurations
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.master_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.master_catalog.type", "hadoop") \
    .config("spark.sql.catalog.master_catalog.warehouse", args['MASTER_S3_PATH']) \
    .config("spark.sql.catalog.curated_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.curated_catalog.type", "hadoop") \
    .config("spark.sql.catalog.curated_catalog.warehouse", args['CURATED_S3_PATH']) \
    .getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read source data
source_df = spark.read.format("csv").option("header", "true").load(args['RAW_S3_PATH'] + args['SOURCE'] + '/' + args['TABLE_NAME'] + '/' + args['PROCESS_TYPE'])

# Data Quality Checks
def perform_data_quality_checks(df, table_name):
    # Example checks
    row_count = df.count()
    if row_count == 0:
        raise ValueError(f"Data quality check failed for {table_name}: No records found")
    else:
        print(f"Data quality check passed for {table_name}: {row_count} records found")
    return df    

# Apply data quality checks
clean_df = perform_data_quality_checks(source_df, args['TABLE_NAME'])

# Write to staging layer
clean_df.write.format("parquet").mode("overwrite").partitionBy("ingestion_date").save(args['STAGING_S3_PATH'] + args['SOURCE'] + '/' + args['TABLE_NAME'] + '/' + args['PROCESS_TYPE'])

# Define Iceberg table path
master_iceberg_table = args['MASTER_S3_PATH'] + args['TABLE_NAME']
curated_iceberg_table = args['CURATED_S3_PATH'] + args['TABLE_NAME']

if args['TABLE_NAME'] == 'customers':
    if args['PROCESS_TYPE'] == 'full':
        # Full load processing for customers
        clean_df = clean_df.withColumn("start_date", current_date()) \
                           .withColumn("end_date", lit(None).cast("date")) \
                           .withColumn("is_current", lit(True))
        
        # Mastering Customers - Type 2 Dimension
        window_spec = Window.partitionBy("customer_id").orderBy(F.desc("effective_date"))
        customers_mastered_df = clean_df.withColumn(
            "row_number", F.row_number().over(window_spec)
        ).withColumn(
            "is_current", when(col("row_number") == 1, lit(True)).otherwise(lit(False))
        ).drop("row_number")

        # Write Mastered Customers to Iceberg Table
        customers_mastered_df.write.mode("overwrite").format("iceberg").save(master_iceberg_table)

        # Write Curated Customers to Iceberg Table
        clean_df.write.format("iceberg").mode("overwrite").partitionBy("region", "effective_date").save(curated_iceberg_table)
    elif args['PROCESS_TYPE'] == 'cdc':
        # CDC processing for customers
        existing_df = spark.read.format("iceberg").load(curated_iceberg_table)
        
        # Mark existing records as not current
        updated_existing_df = existing_df.join(clean_df, "customer_id") \
                                         .withColumn("end_date", current_date()) \
                                         .withColumn("is_current", lit(False))
        
        # Insert new records
        new_records_df = clean_df.withColumn("start_date", current_date()) \
                                 .withColumn("end_date", lit(None).cast("date")) \
                                 .withColumn("is_current", lit(True))
        
        # Merge datasets
        final_df = updated_existing_df.union(new_records_df)
        final_df.write.format("iceberg").mode("append").partitionBy("region", "effective_date").save(curated_iceberg_table)
else:
    partition_column = "category" if args['TABLE_NAME'] == 'products' else "order_date"
    
    # Write to staging layer
    clean_df.write.format("parquet").mode("overwrite").partitionBy("ingestion_date").save(args['STAGING_S3_PATH'] + args['SOURCE'] + '/' + args['TABLE_NAME'] + '/' + args['PROCESS_TYPE'])
    
    # Processing for products and orders
    if args['TABLE_NAME'] == 'products':
        # Mastering Products
        products_mastered_df = clean_df.dropDuplicates(["product_id"])

        # Write Mastered Products to Iceberg Table
        products_mastered_df.write.mode("overwrite").format("iceberg").save(master_iceberg_table)

        # Write Curated Products to Iceberg Table
        products_mastered_df.write.format("iceberg").mode("overwrite").save(curated_iceberg_table)
    elif args['TABLE_NAME'] == 'orders':
        # Calculate total_amount
        augmented_df = clean_df.withColumn("total_amount", col("quantity") * col("price"))

        # Aggregated Sales
        aggregated_sales_df = augmented_df.groupBy("product_id", "order_date").agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("total_amount").alias("total_sales")
        )

        # Write Aggregated Sales to Parquet
        aggregated_sales_df.write.mode("overwrite").partitionBy("order_date").parquet(args['CURATED_S3_PATH'] + 'aggregated_sales')

        # Write Orders to Iceberg Table
        clean_df.write.format("iceberg").mode("overwrite").save(curated_iceberg_table)

# Commit job
job.commit()
