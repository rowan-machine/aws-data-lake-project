import pandas as pd
from utils import generate_customers, generate_products, generate_orders, current_timestamp
from config import NUM_RECORDS

# Generate synthetic data
df_customers = generate_customers(NUM_RECORDS)
df_products = generate_products(NUM_RECORDS)
df_orders = generate_orders(NUM_RECORDS, df_customers['customer_id'].tolist(), df_products['product_id'].tolist())

# Get current timestamp for file naming
timestamp = current_timestamp()

# Save DataFrames as CSV files
df_customers.to_csv(f'customers_{timestamp}.csv', index=False)
df_products.to_csv(f'products_{timestamp}.csv', index=False)
df_orders.to_csv(f'orders_{timestamp}.csv', index=False)
