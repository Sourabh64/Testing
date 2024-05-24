import json
import requests

with open('all_git.json', 'r', encoding='utf8') as f:
    data = json.load(f)
final_list = []
for repo in data:
    members_url = repo["_links"]["members"]
    repo_url = repo['web_url']
    url = members_url+"?private_token=FcTKF6eRu3Ff9KuVMXuU"
    response = requests.get(url)
    resp = response.json()
    member_dict = {"repo_url": repo_url, 'name': [], 'username': [], 'blocked': []}
    for member in resp:
        if member["access_level"] >= 40:
            member_dict['name'].append(member['name'])
            member_dict['username'].append(member['username'])
        if member['state'] != "active":
            member_dict['blocked'].append(member['username'])
    final_list.append(member_dict)

with open("git_member_details.json", "w") as f:
    json.dump(final_list, f)

