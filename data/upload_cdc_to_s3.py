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
upload_to_s3(dev_bucket, 'customers_cdc_2024061906.csv', '01_raw/netsuite/customers/cdc/ingestion_date=2024-06-19-06/customers_cdc_2024061906.csv')
upload_to_s3(dev_bucket, 'products_cdc_2024061906.csv', '01_raw/netsuite/products/cdc/ingestion_date=2024-06-19-06/products_cdc_2024061906.csv')
upload_to_s3(dev_bucket, 'orders_cdc_2024061906.csv', '01_raw/netsuite/orders/cdc/ingestion_date=2024-06-19-06/orders_cdc_2024061906.csv')
