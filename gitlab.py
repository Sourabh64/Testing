import requests
import json


url = f"https://gitlab.payu.in/api/v4/projects?private_token=FcTKF6eRu3Ff9KuVMXuU&pagination=keyset&per_page=50&order_by=id&sort=asc"
response = requests.get(url)
final_response = response.json()
count = 1
while 'Link' in response.headers:
    print(count)
    next_link = response.headers['Link'].split(';')[0].strip("<>")
    response = requests.get(next_link)
    final_response += response.json()
    count += 1

# print(final_response)
with open("all_git.json", "w") as f:
    json.dump(final_response, f)
