import json

import pandas as pd

from json_parser_aws import Parser
from json_parser import Json_Parser_v3

with open('host_details.json', 'r', encoding='utf8') as f:
    response = json.load(f)

# parser = Parser()
# df_dict = parser.process(response, '251_vuln')
parserv3 = Json_Parser_v3()
df_dict = parserv3.process_data(response)


for i in df_dict:
    # print(i)
    # print(df_dict[i])
    df = pd.DataFrame(df_dict[i])
    df.to_csv(i+".csv", index=False)
    # print(df_dict[i])
