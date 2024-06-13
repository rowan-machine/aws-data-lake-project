import boto3

# Initialize S3 client
s3 = boto3.client('s3')

# Define bucket names
dev_bucket = 'ecommerce-data-lake-dev'
prod_bucket = 'ecommerce-data-lake-prod'

# Upload files to S3
def upload_to_s3(bucket, file_name, object_name):
    s3.upload_file(file_name, bucket, object_name)

# Upload CSV files to dev bucket
upload_to_s3(dev_bucket, 'cdc_customers.csv', 'raw-data/netsuite/cdc/customers/customers.csv')
upload_to_s3(dev_bucket, 'cdc_products.csv', 'raw-data/netsuite/cdc/products/products.csv')
upload_to_s3(dev_bucket, 'cdc_orders.csv', 'raw-data/netsuite/cdc/orders/orders.csv')
