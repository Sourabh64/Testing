import json
import subprocess
import charade

with open('all_git.json', 'r') as f:
    data = json.load(f)
final_output = []
for i in data:
    new_dict = {}
    name = i['name']
    git_repo = i['http_url_to_repo']
    val = subprocess.run(f'trufflehog git --json {git_repo}', stdout=subprocess.PIPE)
    coding = charade.detect(val.stdout)
    if val.stdout != b'':
        result = val.stdout.decode(coding['encoding'])
        with open('sample.txt', 'a') as g:
            g.write(result)

