from utils import save_parquet_table

def preprocess_data(spark, df, args, paths):
    # Example preprocessing logic
    df = df.withColumn("preprocessed_column", df["some_column"] * 2)
    save_parquet_table(df, f"{paths['preprocessed']}{args['TABLE_NAME']}/{args['PROCESS_TYPE']}/")
    return df
