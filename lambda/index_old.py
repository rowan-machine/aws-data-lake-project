import boto3
import json
import cfnresponse

def handler(event, context):
    s3 = boto3.client('s3')
    try:
        if event['RequestType'] == 'Create':
            bucket_name = event['ResourceProperties']['BucketName']
            folder_structure = event['ResourceProperties']['FolderStructure']
            
            for folder in folder_structure:
                s3.put_object(Bucket=bucket_name, Key=(folder + '/'))
        
        cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
    
    except Exception as e:
        print(e)
        cfnresponse.send(event, context, cfnresponse.FAILED, {})