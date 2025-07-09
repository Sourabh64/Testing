import json
import requests
import pandas as pd

with open('all_git_users.json', 'r', encoding='utf8') as f:
    data = json.load(f)

ad_df = pd.read_excel('users.xlsx')
git_df = pd.DataFrame(data)


df2_values = set(git_df.values.flatten())
ad_df['Exists_in_df2'] = ad_df['userPrincipalName'].apply(lambda x: x in df2_values)
found = ad_df[ad_df['Exists_in_df2']]
print("Values found in df2:")
print(found)

# Print all results
print("\nUpdated df1:")
print(ad_df)
print(git_df)


