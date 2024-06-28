import pandas as pd
import random
from datetime import datetime
from faker import Faker
import os
from dotenv.main import load_dotenv

load_dotenv()

fake = Faker()

def current_timestamp():
    return datetime.now().strftime('%Y%m%d%H')

def generate_integer_id():
    return random.randint(1000, 9999)

def generate_customers(num_records):
    customers = []
    for _ in range(num_records):
        customers.append({
            'customer_id': generate_integer_id(),
            'name': fake.name(),
            'email': fake.email(),
            'address': fake.address(),
            'signup_date': fake.date_this_decade()
        })
    return pd.DataFrame(customers)

def generate_products(num_records):
    product_names = ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smartwatch']
    categories = ['Electronics', 'Accessories', 'Gadgets']
    products = []
    for _ in range(num_records):
        products.append({
            'product_id': generate_integer_id(),
            'name': random.choice(product_names),
            'category': random.choice(categories),
            'price': round(random.uniform(10.0, 1000.0), 2),
            'stock_quantity': random.randint(1, 100)
        })
    return pd.DataFrame(products)

def generate_orders(num_records, customer_ids, product_ids):
    orders = []
    for _ in range(num_records):
        orders.append({
            'order_id': generate_integer_id(),
            'customer_id': random.choice(customer_ids),
            'product_id': random.choice(product_ids),
            'quantity': random.randint(1, 5),
            'price': round(random.uniform(10.0, 1000.0), 2),
            'order_date': fake.date_this_year()
        })
    return pd.DataFrame(orders)

def upload_to_s3(s3, bucket, file_name, object_name):
    s3.upload_file(file_name, bucket, object_name)
