import json
import boto3
import cfnresponse

s3 = boto3.client('s3')

def lambda_handler(event, context):
    responseData = {}
    try:
        if event['RequestType'] == 'Create':
            bucket_name = event['ResourceProperties']['BucketName']
            folder_structure = event['ResourceProperties']['FolderStructure']
            
            for folder in folder_structure:
                s3.put_object(Bucket=bucket_name, Key=(folder + '/'))
        
        cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)
    except Exception as e:
        responseData['Error'] = str(e)
        cfnresponse.send(event, context, cfnresponse.FAILED, responseData)
