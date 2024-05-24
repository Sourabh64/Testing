import boto3

region_name = 'ap-south-1'
role_arn = 'arn:aws:iam::117458339469:role/Tmura'
session_name = '117458339469'
client = boto3.client('sts',region_name=region_name)
cred = client.assume_role(RoleArn=role_arn,RoleSessionName=session_name)
cred = cred['Credentials']
cred = dict(aws_access_key_id=cred["AccessKeyId"],aws_secret_access_key=cred["SecretAccessKey"],aws_session_token=cred["SessionToken"],region_name=region_name)