def get_kinesis_records(stream_name, shard_id, iterator_type='TRIM_HORIZON'):
    response = kinesis_client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType=iterator_type
    )
    shard_iterator = response['ShardIterator']

    while True:
        response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=100)
        records = response['Records']
        for record in records:
            activity = json.loads(record['Data'])
            print(activity)  # Process the activity data here

        shard_iterator = response['NextShardIterator']
        time.sleep(1)

# Assuming a single shard stream
shard_id = 'shardId-000000000000'
get_kinesis_records(stream_name, shard_id)
