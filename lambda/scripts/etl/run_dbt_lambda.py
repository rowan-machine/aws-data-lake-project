import json
import subprocess

def lambda_handler(event, context):
    result = subprocess.run(["dbt", "run"], capture_output=True, text=True)
    return {
        'statusCode': 200,
        'body': json.dumps(result.stdout)
    }
