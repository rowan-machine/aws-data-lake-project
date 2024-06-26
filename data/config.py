import os
from dotenv.main import load_dotenv

load_dotenv()

DEV_BUCKET = os.getenv('DEV_BUCKET')
PROD_BUCKET = os.getenv('PROD_BUCKET')
NUM_RECORDS = int(os.getenv('NUM_RECORDS'))
