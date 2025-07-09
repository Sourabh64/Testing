import json

import pandas as pd
# from push_to_db import DB
from json_parser_aws import Parser
from json_parser import Json_Parser_v3

with open('jsons/inspector_findings_info_ms_uat_19.json', 'r', encoding='utf8') as f:
    response = json.load(f)

# parser = Parser()
# df_dict = parser.process(response, '251_vuln')
parserv3 = Json_Parser_v3()
# db = DB()
df_dict = parserv3.process_data(response, "info_ms_uat_19")


for i in df_dict:
    # print(i)
    # print(df_dict[i])
    df = pd.DataFrame(df_dict[i])
    # db_response = db.db_process(df_dict)
    df.to_csv("csv/"+i+".csv", index=False)
    # print(df_dict[i])
