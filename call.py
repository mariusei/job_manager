import json
import ssl
import urllib.request as u
import zlib

from my_url_file import url, jobix


# Make sure that `AddOutputFilterByType DEFLATE application/json`
# is enabled on the server side for gzip compression to take place
# (might be useful for large number of jobs)....
jsonhead = {'Content-Type': 'application/json', 'content-encoding' : 'gzip'}
# Specify a SSL context?
context = None # ssl._create_unverified_context()

# This ensures that no other user can hijack the jobs by calling the server.
# The jobix should thus be a secret!
authead = {'jobix': jobix}


#
# Different possible calls to the server at url specified
# in my_url_file.py, working on the job with authentication index
# jobix.
#

#
# Say Hello to the server
#
req = u.Request(f"{url}/")
res = u.urlopen(req, context=context)
print(res.read().decode('utf8'))
res.close()


#
# Initialize a job list
#
jobids = {
        **{ii: {'jobstage': 1} for ii in range(13)},
        }
jobids_json = json.dumps(jobids).encode("utf8")

print("Will submit this job id dict:")
print(jobids)

#
# And define how long a job can reside in a job stage before being
# flagged as potentially broken (worker died+++).
# - This is done through an arbitrarily long list of minutes separated
#   with slashes: `0/10/0/40/0` where `0` indicates infinite time.
#   The jobs are considered to be *running* when in an **even** job stage
#   and *done or waiting for a new worker* in an **odd** numbered job stage.
# - If more than 1024 jobs are submitted, only the first 1024 jobs in the 
#   odd job stages will be printed online.
#
lifetime = '0/1/1'
req = u.Request(f"{url}/init/{lifetime}", headers=authead)
res = u.urlopen(req, context=context)
print('In init job list/lifetimes:', res.read())
res.close()

#
# Set job list
# - A job list saved as JSON and byte-encoded to UTF-8
#   can be uploaded to the server. The jobs will be added if 
#   they don't already exist (job id duplicates are prohibited).
# - Do not try to change jobs with this tool, for that aim, use
#   `{url}/upgrade_job`!
#
req = u.Request(f"{url}/set",
        zlib.compress(jobids_json),
        headers={**jsonhead, **authead},
        )
res = u.urlopen(req, context=context)
print('In set job list:', res.read())
res.close()

#
# Get jobs at this job_stage.
# - Returns 'nada' if no more jobs are available
#
job_stage = 2
req = u.Request(f"{url}/get/{job_stage}", headers=authead)
res = u.urlopen(req, context=context)
print(res)
print(f"Get jobs at job_stage {job_stage}:", json.loads(res.read().decode('utf8')))
res.close()

#
# Checks integrity of job stages
# - If some jobs have lingered in a job stage for longer
#   than specified in lifetime, it becomes `flagged` 
#   for possible resubmission
#
req = u.Request(f"{url}/check_stages", headers=authead)
res = u.urlopen(req, context=context)
print(res.read())
res.close()

#
# Downgrade bad stages
# - Does this only to the jobs that have received a flag
#
oldstage = 2
newstage = 1
req = u.Request(f"{url}/check_stages/{oldstage}/{newstage}", headers=authead)
res = u.urlopen(req, context=context)
print(res.read())
res.close()

#
# Upgrade a job
# - In principle: Do the work and move it to
#   the second stage over the current one
#   as all jobs that are running have their stage incremented by one
#   to ensure that no other worker takes it
#
for i in range(220,300):
    jobid = i
    newstage = 2
    req = u.Request(f"{url}/update_job/{jobid}/{newstage}", headers=authead)
    res = u.urlopen(req, context=context)
    print(res.read())
    res.close()

#
# Upgrade a job AND specify its new/output file path
#
#
jobid = 3
newstage = 2
new_path = json.dumps({'filename': 'out_DR2/set=1/Iloc.npy'}).encode("utf8")
req = u.Request(f"{url}/update_job/{jobid}/{newstage}",
        data=zlib.compress(new_path),
        headers={**authead, **jsonhead})
res = u.urlopen(req, context=context)
print(res.read())
res.close()
