import json
import random
import time

activities = ['view_product', 'add_to_cart', 'remove_from_cart', 'purchase']

def get_random_activity():
    return {
        'customer_id': random.choice(customers_df['customer_id'].tolist()),
        'product_id': random.choice(products_df['product_id'].tolist()),
        'activity': random.choice(activities),
        'timestamp': int(time.time())
    }

while True:
    activity = get_random_activity()
    kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(activity),
        PartitionKey=str(activity['customer_id'])
    )
    time.sleep(1)
