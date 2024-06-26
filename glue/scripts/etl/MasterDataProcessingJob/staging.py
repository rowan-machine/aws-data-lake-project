from utils import get_spark_session, read_config, add_ingestion_date, remove_duplicates, save_parquet_table

def stage_data(glueContext, args):
    config = read_config('config.json')
    spark = get_spark_session("Staging", config['spark']['iceberg_jar_path'], config['spark']['pydeequ_jar_path'], args['MASTER_S3_PATH'])

    raw_path = f"{args['RAW_S3_PATH']}{args['SOURCE']}/{args['TABLE_NAME']}/{args['PROCESS_TYPE']}/"
    df = spark.read.format("csv").option("header", "true").load(raw_path)
    df = add_ingestion_date(df)
    df = remove_duplicates(df, ["order_id"])

    save_parquet_table(df, f"{args['STAGING_S3_PATH']}{args['TABLE_NAME']}/{args['PROCESS_TYPE']}/")
    return df
