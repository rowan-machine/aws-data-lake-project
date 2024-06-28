import os

# Generate full load data
os.system('python generate_full_load_data.py')

# Generate CDC data
os.system('python generate_cdc_data.py')

# Upload full load data to S3
os.system('python upload_full_load_to_s3.py')

# Upload CDC data to S3
os.system('python upload_cdc_to_s3.py')
