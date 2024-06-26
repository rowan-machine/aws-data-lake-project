import pandas as pd
from utils import generate_customers, generate_products, generate_orders, current_timestamp
from config import NUM_RECORDS

# Generate synthetic data
df_customers = generate_customers(NUM_RECORDS)
df_products = generate_products(NUM_RECORDS)
df_orders = generate_orders(NUM_RECORDS, df_customers['customer_id'].tolist(), df_products['product_id'].tolist())

# Save DataFrames as CSV files with consistent names for CDC generation
df_customers.to_csv('customers.csv', index=False)
df_products.to_csv('products.csv', index=False)
df_orders.to_csv('orders.csv', index=False)

# Also save with timestamped names for uploading
timestamp = current_timestamp()
df_customers.to_csv(f'customers_{timestamp}.csv', index=False)
df_products.to_csv(f'products_{timestamp}.csv', index=False)
df_orders.to_csv(f'orders_{timestamp}.csv', index=False)
