import os
from dotenv import load_dotenv

load_dotenv()

ENVIRONMENT = os.getenv('ENVIRONMENT')
DEV_BUCKET = os.getenv('DEV_BUCKET')
PROD_BUCKET = os.getenv('PROD_BUCKET')
NUM_RECORDS = int(os.getenv('NUM_RECORDS'))

if ENVIRONMENT == 'prod':
    BUCKET = PROD_BUCKET
else:
    BUCKET = DEV_BUCKET
