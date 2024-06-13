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
upload_to_s3(dev_bucket, 'customers.csv', 'raw-data/netsuite/full-load/customers/customers.csv')
upload_to_s3(dev_bucket, 'products.csv', 'raw-data/netsuite/full-load/products/products.csv')
upload_to_s3(dev_bucket, 'orders.csv', 'raw-data/netsuite/full-load/orders/orders.csv')
