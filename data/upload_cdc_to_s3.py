import boto3
from utils import current_timestamp, upload_to_s3
from config import DEV_BUCKET

# Initialize S3 client
s3 = boto3.client('s3')

# Get current timestamp for file naming
timestamp = current_timestamp()

# Upload CSV files to dev bucket
upload_to_s3(s3, DEV_BUCKET, f'customers_cdc_{timestamp}.csv', f'01_raw/netsuite/customers/cdc/ingestion_date={timestamp}/customers_cdc_{timestamp}.csv')
upload_to_s3(s3, DEV_BUCKET, f'products_cdc_{timestamp}.csv', f'01_raw/netsuite/products/cdc/ingestion_date={timestamp}/products_cdc_{timestamp}.csv')
upload_to_s3(s3, DEV_BUCKET, f'orders_cdc_{timestamp}.csv', f'01_raw/netsuite/orders/cdc/ingestion_date={timestamp}/orders_cdc_{timestamp}.csv')
