import json
import ssl
import urllib.request as u

jobids = {
        '12': {'jobstage': 2},
        '3': {'jobstage': 1},
        '5': {'jobstage': 2},
        '7': {'jobstage': 0},
        }
jobids_json = json.dumps(jobids).encode("utf8")

url = 'http://localhost:5000'
jobix = 1234
jsonhead = {'Content-Type': 'application/json'}
context = None #ssl._create_unverified_context()
authead = {'jobix': jobix}

# Hello
req = u.Request(f"{url}/")
res = u.urlopen(req, context=context)
print(res.read().decode('utf8'))
res.close()

# Init job list
lifetime = '0/1/1'
req = u.Request(f"{url}/init/{lifetime}", headers=authead)
res = u.urlopen(req, context=context)
print('In init job list/lifetimes:', res.read())
res.close()

# Set job list
req = u.Request(f"{url}/set",
        jobids_json,
        headers={**jsonhead, **authead},
        )
res = u.urlopen(req, context=context)
print('In set job list:', res.read())
res.close()

# Get job list
job_stage = 2
req = u.Request(f"{url}/get/{job_stage}", headers=authead)
res = u.urlopen(req, context=context)
print(res)
print(f"Get jobs at job_stage {job_stage}:", json.loads(res.read().decode('utf8')))
#res.close()

# Check job stages
req = u.Request(f"{url}/check_stages", headers=authead)
res = u.urlopen(req, context=context)
print(res.read())
res.close()

## Downgrade bad stages
#oldstage = 2
#newstage = 1
#req = u.Request(f"{url}/check_stages/{oldstage}/{newstage}", headers=authead)
#res = u.urlopen(req, context=context)
#print(res.read())
#res.close()

# Upgrade a job
jobid = 5 
newstage = 2
req = u.Request(f"{url}/update_job/{jobid}/{newstage}", headers=authead)
res = u.urlopen(req, context=context)
print(res.read())
res.close()

# Upgrade a job AND specify its new file
jobid = 3
newstage = 2
new_path = json.dumps({'filename': 'out_DR2/set=1/Iloc.npy'}).encode("utf8")
req = u.Request(f"{url}/update_job/{jobid}/{newstage}",
        data=new_path,
        headers={**authead, **jsonhead})
res = u.urlopen(req, context=context)
print(res.read())
res.close()
