import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SQLContext

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read the CDC data from S3
cdc_orders = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://ecommerce-data-lake-us-east-1-dev/raw-data/netsuite/cdc/orders/"]},
    format="csv"
)

cdc_customers = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://ecommerce-data-lake-us-east-1-dev/raw-data/netsuite/cdc/customers/"]},
    format="csv"
)

cdc_products = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://ecommerce-data-lake-us-east-1-dev/raw-data/netsuite/cdc/products/"]},
    format="csv"
)

# Convert Glue DynamicFrame to Spark DataFrame
df_cdc_orders = cdc_orders.toDF()
df_cdc_customers = cdc_customers.toDF()
df_cdc_products = cdc_products.toDF()

# Use the Glue database
spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_db_dev")
spark.sql("USE ecommerce_db_dev")

df_cdc_orders.createOrReplaceTempView("cdc_orders")
df_cdc_customers.createOrReplaceTempView("cdc_customers")
df_cdc_products.createOrReplaceTempView("cdc_products")

# Paths for processed data
processed_orders_path = "s3://ecommerce-data-lake-us-east-1-dev/processed-data/merged/orders/"
processed_customers_path = "s3://ecommerce-data-lake-us-east-1-dev/processed-data/merged/customers/"
processed_products_path = "s3://ecommerce-data-lake-us-east-1-dev/processed-data/merged/products/"

# Merge Orders
spark.sql(f"""
MERGE INTO orders t
USING cdc_orders s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
# Write the merged data to the processed data location
spark.sql(f"SELECT * FROM orders").write.format("iceberg").mode("overwrite").save(processed_orders_path)

# Merge Customers
spark.sql(f"""
MERGE INTO customers t
USING cdc_customers s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
# Write the merged data to the processed data location
spark.sql(f"SELECT * FROM customers").write.format("iceberg").mode("overwrite").save(processed_customers_path)

# Merge Products
spark.sql(f"""
MERGE INTO products t
USING cdc_products s
ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
# Write the merged data to the processed data location
spark.sql(f"SELECT * FROM orders").write.format("iceberg").mode("overwrite").save(processed_orders_path)
spark.sql(f"SELECT * FROM customers").write.format("iceberg").mode("overwrite").save(processed_customers_path)
spark.sql(f"SELECT * FROM products").write.format("iceberg").mode("overwrite").save(processed_products_path)

job.commit()
