import boto3
import pandas as pd
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# Generate synthetic data
def generate_data(num_records):
    orders = []
    customers = []
    products = []

    for _ in range(num_records):
        customer_id = fake.uuid4()
        product_id = fake.uuid4()

        customers.append({
            'customer_id': customer_id,
            'name': fake.name(),
            'email': fake.email(),
            'address': fake.address(),
            'signup_date': fake.date_this_decade()
        })

        products.append({
            'product_id': product_id,
            'name': fake.word(),
            'category': fake.word(),
            'price': round(random.uniform(10.0, 100.0), 2),
            'stock_quantity': random.randint(1, 100)
        })

        orders.append({
            'order_id': fake.uuid4(),
            'customer_id': customer_id,
            'product_id': product_id,
            'quantity': random.randint(1, 5),
            'price': round(random.uniform(10.0, 100.0), 2),
            'order_date': fake.date_this_year()
        })

    return customers, products, orders

# Generate 1000 records
customers, products, orders = generate_data(1000)

# Convert to DataFrame
df_customers = pd.DataFrame(customers)
df_products = pd.DataFrame(products)
df_orders = pd.DataFrame(orders)

# Save DataFrames as CSV files
df_customers.to_csv('customers_20240619.csv', index=False)
df_products.to_csv('products_20240619.csv', index=False)
df_orders.to_csv('orders_20240619.csv', index=False)