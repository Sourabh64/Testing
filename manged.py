import time
import json
import boto3
import pandas as pd
from json_parser_aws import Parser
# from push_to_db import DB


class AWS:
    def __init__(self):
        pass

    def get_data(self, df):
        region_names = ['ap-south-1', 'us-east-1']
        # df = pd.read_csv('Cloud_awscloud_202209211754.csv')
        temp_list = []
        for index, row in df.iterrows():
            role_arn = row['arn']
            session_name = str(row['account_id'])
            for region in region_names:
                try:
                    client = boto3.client('sts', region_name=region)
                    cred = client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
                    cred = cred['Credentials']
                    try:
                        cred = dict(aws_access_key_id=cred["AccessKeyId"], aws_secret_access_key=cred["SecretAccessKey"], aws_session_token=cred["SessionToken"], region_name=region)
                        db_client = boto3.client('rds', **cred)
                        db_dict = db_client.describe_db_clusters()
                        db_dict['account_id'] = session_name
                        temp_list.append(db_dict)
                    except Exception as e:
                        print(e)
                except Exception as e:
                    print(e)
        return temp_list


if __name__ == '__main__':
    start = time.time()
    # aws = AWS()
    # db = DB()
    # conn, cursor = db.connect()
    # query = """select arn, account_id from aws.cloud_details where cloud = 'AWS'"""
    # resp = db.execute_query(cursor, query)
    # df = pd.DataFrame(resp, columns=["arn", "account_id"])
    # response = aws.get_data(df)
    with open('jsons/managed_devices.json', 'r', encoding='utf8') as f:
        data = json.load(f)
    response = data
    parser = Parser()
    df_dict = parser.process(response["value"], 'mds')
    count = 0
    from datetime import date
    today = date.today()
    for i in df_dict:
        count += 1
        df_dict[i]['date'] = pd.to_datetime(today)
        df_dict[i].to_csv(f'csvs/{i}.csv', index=False)
    # db_response = db.db_process(df_dict)
