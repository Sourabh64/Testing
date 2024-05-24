import json
import pandas as pd
with open('jsons/trufflehog_git_new_one.json', 'r', encoding='utf8') as f:
    data = json.load(f)

df = pd.read_csv("csvs/git.csv")
for column in df:
    if 'http_url_to_repo' == column:
        git_repos = df['http_url_to_repo'].values.tolist()

final_data = {}
for i in data:
    print(i)
    # if i['SourceMetadata']['Data']['Git']['repository']