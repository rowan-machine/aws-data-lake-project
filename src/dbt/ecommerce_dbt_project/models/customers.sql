-- models/customers.sql
with raw_customers as (
  select * from ecommerce_db_dev.customers
),
clean_customers as (
  select
    customer_id,
    name,
    email,
    address,
    signup_date
  from raw_customers
  where email is not null
)
select * from clean_customers
