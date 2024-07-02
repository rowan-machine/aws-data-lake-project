import pandas as pd

# Example of feature engineering with the order data
orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])

# Customer lifetime value
customer_lifetime_value = orders_df.groupby('customer_id')['price'].sum().reset_index()
customer_lifetime_value.columns = ['customer_id', 'lifetime_value']

# Average order value
average_order_value = orders_df.groupby('customer_id')['price'].mean().reset_index()
average_order_value.columns = ['customer_id', 'avg_order_value']

# Purchase frequency
purchase_frequency = orders_df.groupby('customer_id')['order_id'].count().reset_index()
purchase_frequency.columns = ['customer_id', 'purchase_frequency']

# Merging features into customers data
customers_features = customers_df.merge(customer_lifetime_value, on='customer_id', how='left')
customers_features = customers_features.merge(average_order_value, on='customer_id', how='left')
customers_features = customers_features.merge(purchase_frequency, on='customer_id', how='left')

import ace_tools as tools; tools.display_dataframe_to_user(name="Customer Features", dataframe=customers_features)

customers_features.head()
