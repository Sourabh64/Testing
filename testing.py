# def solution(area):
#     output_list = []
#     while area > 0:
#         val = int(area ** (1 / 2))
#         nearest_val = val ** 2
#         area = area - nearest_val
#         output_list.append(nearest_val)
#     return output_list
#
#
# output = solution(12)
# print(output)

import requests
from requests.auth import HTTPBasicAuth

import json

url = "http://10.225.5.221:9010/api/projects/search?ps=500&p=1"


response = requests.request("GET", url, auth=HTTPBasicAuth("13447da15bec677be81935e778ae47d6f54ba7a3", ""))

resp = response.json()["components"]
for project in resp:
    print(project["key"], project["name"])
    url = "http://10.225.5.221:9010/api/"

    export_findings_for_project(project=key)
