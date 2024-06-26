import pandas as pd
from utils import current_timestamp, generate_integer_id, fake

# Load existing data from fixed file names
df_customers = pd.read_csv('customers.csv')
df_products = pd.read_csv('products.csv')
df_orders = pd.read_csv('orders.csv')

def generate_cdc_data(df, record_type):
    cdc_data = []
    for _, row in df.iterrows():
        event_type = random.choice(['INSERT', 'UPDATE', 'DELETE'])
        record = row.to_dict()
        record['event_type'] = event_type
        
        if record_type == 'customers':
            if event_type == 'INSERT':
                record['customer_id'] = generate_integer_id()
                record['name'] = fake.name()
                record['email'] = fake.email()
                record['address'] = fake.address()
                record['signup_date'] = fake.date_this_decade()
            elif event_type == 'UPDATE':
                record['name'] = fake.name()
        elif record_type == 'products':
            if event_type == 'INSERT':
                record['product_id'] = generate_integer_id()
                record['name'] = random.choice(['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smartwatch'])
                record['category'] = random.choice(['Electronics', 'Accessories', 'Gadgets'])
                record['price'] = round(random.uniform(10.0, 1000.0), 2)
                record['stock_quantity'] = random.randint(1, 100)
            elif event_type == 'UPDATE':
                record['price'] = round(random.uniform(10.0, 1000.0), 2)
                record['stock_quantity'] = random.randint(1, 100)
        elif record_type == 'orders':
            if event_type == 'INSERT':
                record['order_id'] = generate_integer_id()
                record['customer_id'] = random.choice(df_customers['customer_id'].tolist())
                record['product_id'] = random.choice(df_products['product_id'].tolist())
                record['quantity'] = random.randint(1, 5)
                record['price'] = round(random.uniform(10.0, 1000.0), 2)
                record['order_date'] = fake.date_this_year()
            elif event_type == 'UPDATE':
                record['quantity'] = random.randint(1, 5)
                record['price'] = round(random.uniform(10.0, 1000.0), 2)
        
        cdc_data.append(record)

    return pd.DataFrame(cdc_data)

# Generate CDC data
cdc_customers = generate_cdc_data(df_customers, 'customers')
cdc_products = generate_cdc_data(df_products, 'products')
cdc_orders = generate_cdc_data(df_orders, 'orders')

# Get current timestamp for file naming
timestamp = current_timestamp()

# Save CDC DataFrames as CSV files with timestamped names
cdc_customers.to_csv(f'customers_cdc_{timestamp}.csv', index=False)
cdc_products.to_csv(f'products_cdc_{timestamp}.csv', index=False)
cdc_orders.to_csv(f'orders_cdc_{timestamp}.csv', index=False)
