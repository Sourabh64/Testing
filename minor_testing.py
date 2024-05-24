# import re
#
# import requests
# import json
# import time
#
# # from requests.packages.urllib3.exceptions import InsecureRequestWarning
#
#
# # requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
#
# url = "https://cloud.tenable.com/scans"
# start = time.time()
# headers = {
#   'X-ApiKeys': 'accessKey=b6aefc1154351f16678d3086d566408b084c3fe43cf2d4668173a5094ac4a807;secretKey'
#                '=1443a632ff99bca008752ae72c07b61d1def96814697afda0ec28e8e39cc3209;',
#   # 'Cookie': 'nginx-cloud-site-id=eu-fra-1'
# }
#
# response = requests.request("GET", url, headers=headers, verify=False)
#
# resp = response.json()
# scans = resp['scans']
# final_list = []
# failed_list = []
# for scan in scans:
#     if "@payu.in" in scan['owner']:
#         id = scan['id']
#         url = f"https://cloud.tenable.com/scans/{id}"
#         response = requests.request("GET", url, headers=headers, verify=False)
#         if response.status_code == 200:
#             resp = response.json()
#             final_list.append(resp)
#         else:
#             failed_list.append(scan)
# with open("teanble_detailed_data.json", "w") as f:
#     json.dump(final_list, f)
# print(time.time()-start)

project_list = ['COCR-', 'PAYUAP-', 'MS-', 'CRBK-', 'PART-', 'REF-', 'BBPCNCT-', 'V2P']
if 'COCR-' in 'COCR-109':
    print("yes")
else:
    print("No")