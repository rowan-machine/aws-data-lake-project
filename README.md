# aws-data-lake-project
All-in-one repository for AWS CloudFormation, DBT, Glue, and Athena

## Deploying CloudFormation Template

To deploy the CloudFormation template using the parameters file, use the following AWS CLI command:

```bash
aws cloudformation create-stack --stack-name DataLakeGlueStack --template-body file://cloudformation/data-lake-glue-template.yaml --parameters file://cloudformation/parameters.json --capabilities CAPABILITY_NAMED_IAM
