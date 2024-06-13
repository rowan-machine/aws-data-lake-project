import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Sample transformation
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "ecommerce_db_dev", table_name = "orders", transformation_ctx = "datasource0")
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("order_id", "string", "customer_id", "string", "product_id", "string", "quantity", "int", "price", "float", "order_date", "date")], transformation_ctx = "applymapping1")
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://ecommerce-data-lake-dev/raw-data/netsuite/cdc/orders/"}, format = "json", transformation_ctx = "datasink2")
job.commit()