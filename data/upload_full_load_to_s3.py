import boto3

# Initialize S3 client
s3 = boto3.client('s3')

# Define bucket names
dev_bucket = 'ecommerce-data-lake-us-east-1-dev'
prod_bucket = 'ecommerce-data-lake-us-east-1-prod'

# Upload files to S3
def upload_to_s3(bucket, file_name, object_name):
    s3.upload_file(file_name, bucket, object_name)

# Upload CSV files to dev bucket
upload_to_s3(dev_bucket, 'customers_20240619.csv', '01_raw/netsuite/customers/full_load/ingestion_date=2024-06-19/customers_20240619.csv')
upload_to_s3(dev_bucket, 'products_20240619.csv', '01_raw/netsuite/products/full_load/ingestion_date=2024-06-19/products_20240619.csv')
upload_to_s3(dev_bucket, 'orders_20240619.csv', '01_raw/netsuite/orders/full_load/ingestion_date=2024-06-19/orders_20240619.csv')
