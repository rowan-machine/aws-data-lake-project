import pandas as pd
import random
from faker import Faker

# Initialize Faker
fake = Faker()

# Load existing data
df_customers = pd.read_csv('customers.csv')
df_products = pd.read_csv('products.csv')
df_orders = pd.read_csv('orders.csv')

# Function to generate CDC data
def generate_cdc_data(df, record_type):
    cdc_data = []

    for index, row in df.iterrows():
        event_type = random.choice(['INSERT', 'UPDATE', 'DELETE'])
        record = row.to_dict()
        record['event_type'] = event_type
        
        if record_type == 'customers':
            if event_type == 'INSERT':
                record['customer_id'] = fake.uuid4()  # For new insert
                record['name'] = fake.name()
                record['email'] = fake.email()
                record['address'] = fake.address()
                record['signup_date'] = fake.date_this_decade()
            elif event_type == 'UPDATE':
                record['name'] = fake.name()  # Simulate update
        elif record_type == 'products':
            if event_type == 'INSERT':
                record['product_id'] = fake.uuid4()  # For new insert
                record['name'] = fake.word()
                record['category'] = fake.word()
                record['price'] = round(random.uniform(10.0, 100.0), 2)
                record['stock_quantity'] = random.randint(1, 100)
            elif event_type == 'UPDATE':
                record['price'] = round(random.uniform(10.0, 100.0), 2)  # Simulate update
                record['stock_quantity'] = random.randint(1, 100)
        elif record_type == 'orders':
            if event_type == 'INSERT':
                record['order_id'] = fake.uuid4()  # For new insert
                record['customer_id'] = random.choice(df_customers['customer_id'].tolist())
                record['product_id'] = random.choice(df_products['product_id'].tolist())
                record['quantity'] = random.randint(1, 5)
                record['price'] = round(random.uniform(10.0, 100.0), 2)
                record['order_date'] = fake.date_this_year()
            elif event_type == 'UPDATE':
                record['quantity'] = random.randint(1, 5)  # Simulate update
                record['price'] = round(random.uniform(10.0, 100.0), 2)
        # 'DELETE' will have the same data as existing row but marked for deletion
        cdc_data.append(record)

    return pd.DataFrame(cdc_data)

# Generate CDC data for customers, products, and orders
cdc_customers = generate_cdc_data(df_customers, 'customers')
cdc_products = generate_cdc_data(df_products, 'products')
cdc_orders = generate_cdc_data(df_orders, 'orders')

# Save CDC DataFrames as CSV files
cdc_customers.to_csv('customers_cdc_2024061906.csv', index=False)
cdc_products.to_csv('products_cdc_2024061906.csv', index=False)
cdc_orders.to_csv('orders_cdc_2024061906.csv', index=False)
