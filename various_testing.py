import json
import subprocess
import charade

with open('jsons/all_git.json', 'r') as f:
    data = json.load(f)
final_output = []
for i in data:
    # new_dict = {}
    # name = i['name']
    # git_repo = i['http_url_to_repo']
    git_repo = 'https://gitlab.payu.in/payu/payu.git'
    # val = subprocess.run(f'trufflehog git {git_repo}', stdout=subprocess.PIPE)
    val = subprocess.Popen(f'trufflehog git {git_repo}', shell=True, stdout=subprocess.PIPE)
    stdout, stderr = val.communicate()
    # val = subprocess.call(["trufflehog", "git", "--json", git_repo], capture_output=True)
    if stdout != b'':
        coding = charade.detect(stdout)
        result = stdout.decode(coding['encoding'])
        print(result['Raw'])
        with open('store_card_test.txt', 'a') as g:
            g.write(result)
    break


