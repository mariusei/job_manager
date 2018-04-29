import json
import ssl
import urllib.request as u
import zlib

from my_url_file import url, jobix

#
# Example configuration of a worker
# that only asks for jobs until there are no more at the server side
# (server responds 'nada').
#

jsonhead = {'Content-Type': 'application/json', 'content-encoding' : 'gzip'}
context = None #ssl._create_unverified_context()
authead = {'jobix': jobix}

def ask_for_job(job_stage):
    req = u.Request(f"{url}/get/{job_stage}", headers=authead)
    res = u.urlopen(req, context=context)
    info = json.loads(res.read().decode('utf8'))
    res.close()
    print('info', info)

    return info

def update_job(jobid, next_stage):
    req = u.Request(f"{url}/update_job/{jobid}/{next_stage}", headers=authead)
    res = u.urlopen(req, context=context)
    # Should verify that the job was accepted
    print(res.read())
    res.close()


free_jobs = True
# Get a free job id at this job stage:
job_stage = 1
newstage = 2

maxi = 2
i = 0

print("Asking for jobs...")
while free_jobs and i < maxi:
    i += 1
    # Check if there are jobs
    info = ask_for_job(job_stage)
    if info != 'nada':
        jobid = info['jobid']
        jobstage = info['jobstage']
        #jobix = info['jobix']
        print(f"Working on jobid {jobid} at stage {jobstage}")
        
        # Upgrade this job
        update_job(jobid, newstage)
        print(f"Updated jobid {jobid} to stage {newstage}")
    else:
        free_jobs = False



