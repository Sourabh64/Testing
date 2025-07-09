import io
import json
import subprocess
import truffleHog

final_output = {}
cmd = ['trufflehog', 'git', '--json', 'https://gitlab.payu.in/payu/olap.git']
output = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]
print(output)
output = output.decode('utf-8')
final_output['output'] = output
new_json = json.loads(output)
with open('git_repo_output.json', 'w') as f:
    json.dump(final_output, f)

