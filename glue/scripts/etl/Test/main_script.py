import staging

def main():
    # Parameters
    raw_s3_path = "s3://ecommerce-data-lake-us-east-1-dev/01_raw/"
    staging_s3_path = "s3://ecommerce-data-lake-us-east-1-dev/02_staging/"
    table_name = "orders"

    # Stage data
    staging.stage_data(spark, raw_s3_path, staging_s3_path, table_name)
    # Add calls to other modules here
    pass

if __name__ == "__main__":
    main()
