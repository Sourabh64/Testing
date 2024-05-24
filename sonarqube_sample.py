# from sonarqube import SonarCloudClient
# sonarcloud_url = "http://10.225.5.221:9010"
# sonarcloud_token = "13447da15bec677be81935e778ae47d6f54ba7a3"
# sonar = SonarCloudClient(sonarcloud_url=sonarcloud_url, token=sonarcloud_token)
# projects = sonar.projects.export_findings_for_project(project="PAYUAP-264")
# print()

import requests
import json
import time

with open('sonarqube_projects2.json', 'r', encoding='utf8') as f:
    data = json.load(f)
project_list = ['COCR-', 'PAYUAP-', 'MS-', 'CRBK-', 'PART-', 'REF-', 'BBPCNCT-', 'V2P']
for i in data['components']:
    if 'MS-' in i['key']:
        project = i['key']
        url = f"http://10.225.5.221:9010/api/project_dump/export?key={project}"
        headers = {
            'Authorization': 'Basic MTM0NDdkYTE1YmVjNjc3YmU4MTkzNWU3NzhhZTQ3ZDZmNTRiYTdhMzo=',
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers)
        if response.status_code == 200:
            print(i['key'])
            time.sleep(3)
            delete_URL = f"http://10.225.5.221:9010/api/projects/delete?project={project}"
            response_del = requests.request("POST", delete_URL, headers=headers)
            if response_del.status_code == 204:
                pass
            else:
                print(f"Failed to delete {project}")
        else:
            print(f"Failed to export {project}")


# print(response.text)
