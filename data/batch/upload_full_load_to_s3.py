import boto3
from utils import current_timestamp, upload_to_s3
from config import BUCKET

# Initialize S3 client
s3 = boto3.client('s3')

# Get current timestamp for file naming
timestamp = current_timestamp()

# Upload CSV files to the configured bucket
upload_to_s3(s3, BUCKET, f'customers_{timestamp}.csv', f'01_raw/netsuite/customers/full_load/ingestion_date={timestamp}/customers_{timestamp}.csv')
upload_to_s3(s3, BUCKET, f'products_{timestamp}.csv', f'01_raw/netsuite/products/full_load/ingestion_date={timestamp}/products_{timestamp}.csv')
upload_to_s3(s3, BUCKET, f'orders_{timestamp}.csv', f'01_raw/netsuite/orders/full_load/ingestion_date={timestamp}/orders_{timestamp}.csv')
