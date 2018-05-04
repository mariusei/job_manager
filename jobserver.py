
import json
import zlib
import os
import datetime

from flask import Flask, request, g
from flask import render_template
from flask import abort
#from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO

import pmdb
import glob


app = Flask(__name__, instance_relative_config=True)
socketio = SocketIO(app)

# Job list SQLite database
#app.config['SQLALCHEMY_DATABASE_URI'] = "sqlite:///joblist.db"
#app.config['SQLALCHEMY_BINDS'] = {
#        'lifetimes': "sqlite:///lifetimes.db"
#        }
#app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

dateformat = "%Y-%m-%d %H:%M:%S"
lt_file_ = "lifetimes_{jobix}.json"
db_file_ = "database_{jobix}.pmem"
job_len_file_ = "joblen_{jobix}.json"

# List of jobs that have been pushed to web page
jobs_on_webpage = []

#db = SQLAlchemy(app)

def set_or_update_job_len(jobix, n):
    # Check if length of jobs already have been set?
    with open(job_len_file_.format(jobix=jobix), "w") as fout:
        json.dump({'njobs': n}, fout)

def get_job_len(jobix):
    if os.path.exists(job_len_file_.format(jobix=jobix)):
        with open(job_len_file_.format(jobix=jobix), "r") as fin:
            n = int(json.load(fin)['njobs'])
    else:
        n = 0
    return n

def set_lifetimes(jobix, lifetimes):
    with open(lt_file_.format(jobix=jobix), "w") as fout:
        json.dump(lifetimes, fout)

def get_lifetimes(jobix):
    if os.path.exists(lt_file_.format(jobix=jobix)):
        with open(lt_file_.format(jobix=jobix), "rb") as fin:
            lts = json.load(fin)
    else :
        lts = []
    return lts

def update_lifetimes(jobix, stage, lifetime):
    if os.path.exists(lt_file_.format(jobix=jobix)):
        with open(lt_file_.format(jobix=jobix), "r") as fin:
            lts = json.load(fin)
        # Change the lifetime at this stage
        lts[stage] = lifetime
        # Save updated json file:
        with open(lt_file_.format(jobix=jobix), "w") as fout:
            json.dump(lifetimes, fout)
    else :
        lts = []
    return lts


"""
"
" init
"
"""
def init():
    """ Checks if 'globals' have been defined
    """
    try:
        g.njobs
    except AttributeError:
        g.njobs = 0

    try:
        g.isworking
    except AttributeError:
        g.isworking = False


"""
"
" get_jobs
"
"""
@app.route("/")
def hello():
    # Initialize files if they don't exist
    # and have data
    #init_db_if_not_existing()
    init()
    return "Hello World!"

"""
"
" init_ix
"
"""
@app.route("/init")
@app.route("/init/<path:stage_times>")
def init_ix(stage_times=None):
    """ Initializes lifeties from
        /init/0_in_min/.../i_n_min/.../n_in_min
        for each of the i_in_min stages
        where the i_in_min is given in minutes.

        0 lifetime indicates infinity
    """
    init()

    if 'jobix' in request.headers:
        jobix = int(request.headers['jobix'])
    else:
        abort(403, "jobix is missing from headers!")

    if stage_times and not g.isworking:
        lifetimes = stage_times.split('/')
        n_stages = len(lifetimes)

        # Verify that the lifetimes haven't already been set
        # (they can be changed with another function)
        #if not Lifetime.query.filter_by(jobix=jobix).first():
        #    # add these to the database
        #    for i, lt in enumerate(lifetimes):
        #        lt = int(lt)
        #        entry = Lifetime(
        #                jobix = jobix,
        #                jobstage = i,
        #                lifetime = lt
        #                )
        #        db.session.add(entry)
        #        db.session.commit()
        #lts_q = Lifetime.query.all()
        #lts_new = {i.jobstage: i.lifetime for i in lts_q}

        # New lifetimes:
        g.lifetimes = lifetimes
        lts_new = json.dumps(g.lifetimes).encode("utf8")

        # And store it to file
        lt_file = lt_file_.format(jobix=jobix)
        print(f"does {lt_file} exist? {os.path.exists(lt_file)}")
        print(f"{lt_file} abspath: {os.path.abspath(lt_file)}")
        if not os.path.exists(lt_file) or os.stat(lt_file).st_size ==0:
            with open(lt_file, "w") as fout:
                json.dump(lifetimes, fout)
            # Set global:
        else:
            # Verify that old/new lifetimes match
            with open(lt_file, "rb") as fin:
                lts_old = json.load(fin)
            assert lifetimes == lts_old, ("Lifetimes don't match!", lifetimes, lts_old)

        # Reload web pages
        bcast_reload()
        bcast_jobstage_counter()
        return lts_new
    elif not stage_times and not g.isworking:
        # Reload web pages
        bcast_reload()
        bcast_jobstage_counter()
        if len(glob.glob(lt_file_.replace("{job_ix}", "*"))) > 0:
            jobix_ = int(glob.glob(lt_file_.replace("{jobix}","*"))[0].split("_")[1].split(".")[0])

            lts = get_lifetimes(jobix_)
        else :
            lts = []

        #return json.dumps(Lifetime.query.all()).encode("utf8")
        return json.dumps(lts).encode("utf8")
    
    else:
        return json.dumps("busy").encode("utf8")

"""
"
" set_jobs
"
"""
@app.route("/set", methods=['POST'])
def set_jobs():
    """ Sets a job entry/entries to jobix,
        reads jobix from encrypted header
        and jobids, jobstage from serialized
        json dictionary.
    """

    init()

    if g.isworking:
        return json.dumps("busy").encode("utf8")

    if request.method == 'POST':
        if 'jobix' in request.headers:
            jobix = int(request.headers['jobix'])
        else:
            abort(403, "jobix must be specified in header!")

        g.isworking = True
        init_db_if_not_existing(jobix)

        ids = json.loads(zlib.decompress(request.data))

        # Insert into pmdb
        # assume one or all arrive simultaneously,
        # but they must be in a list
        t_now = datetime.datetime.now().strftime(dateformat)

        print("RECEIVED JOBS AND ARE PROCESSING THEM!")
        print(ids.keys())

        #print(ids['jobid'])
        #print(ids['job'])
        #print(ids['jobtagged'])

        #job = ids['job'] if 'job' in ids else None
        #jobstage = ids['jobstage'] if 'jobstage' in ids else None
        #jobdatecommitted = ids['jobdatecommitted'] if 'jobdatecommitted' in ids else None
        #jobpath = ids['jobpath'] if 'jobpath' in ids else None
        #jobtagged = ids['jobtagged'] if 'jobtagged' in ids else None

        old_njobs = get_job_len(jobix) 

        jobid = ids['jobid']
        if 'n_max' in ids:
            n_max = int(ids['n_max'])
            print(f"Will add jobs until id {n_max}")
        else:
            n_max = len(jobid)
        job = [json.dumps(j).encode('utf8') for j in ids['job']]
        if 'jobstage' in ids:
            jobstage = [int(j) for j in ids['jobstage']]
        else:
            jobstage = [0 for _ in range(len(jobid))]
        if 'jobpath' in ids:
            jobpath = [f"{ids['jobpath'][j]}".encode('utf8') for j in range(len(job))]
        else:
            jobpath = ["".encode('utf8') for _ in range(len(job))]
        if 'jobdatecommitted' in ids:
            jobpath = [f"{ids['jobdatecommitted'][j]}".encode('utf8') for j in range(len(job))]
        else:
            jobdatecommitted = [datetime.datetime.now().strftime(dateformat).encode('utf8') \
                for _ in range(len(jobid))]
        if 'jobtagged' in ids:
            jobstage = [int(j) for j in ids['jobtagged']]
        else:
            jobtagged = [0 for _ in range(len(jobid))]

        status = pmdb.insert(
                path_in = db_file_.format(jobix=jobix),
                n_max = n_max,
                jobid = jobid,
                job = job,
                jobstage = jobstage,
                jobpath = jobpath,
                jobdatecommitted = jobdatecommitted,
                jobtagged = jobtagged
                )
        # Bad: won't be logged on the server, impossible to debug!
        #except AttributeError as e:
        #    abort(400, f"Invalid request to pmdb.insert:  {e}")

        print("ADDED JOBS!")
        print(status)

        #set_or_update_job_len(jobix, len(job))
        #bcast_reload()
        #bcast_jobstage_counter()

        g.isworking = False
        print("post accepted!")
        return f"Accepted to {jobix}!"
    else:
        print("post rejected!")
        return f"Not understood."

"""
"
" update_job
"
"""
@app.route("/update_job/<int:jobid>/<int:jobstage>", methods=['GET', 'POST'])
def update_job(jobid=None, jobstage=None):
    """ Updates the job stage if authorized to do so """
    if 'jobix' in request.headers:
        jobix = int(request.headers['jobix'])
    else:
        abort(403, "jobix must be specified in header!")

    init()
    if not g.isworking:
        init_db_if_not_existing(jobix)

        #print("Updated DB")

        job = None
        filename =  None
        jobtime = f"{datetime.datetime.now().strftime(dateformat)}".encode("utf8")
        jobtagged = -1

        if request.method == 'POST':
            # We could receive both a save path and a new unpickleable job command
            # that the worker for the next step can use for its
            # analysis
            req = json.loads(zlib.decompress(request.data))
            if 'filename' in req:
                filename = req['filename'].encode("utf8")
                fn_screen = filename.decode('utf8')
            else:
                filename = None
            if 'jobcmd' in req:
                jobcmd = req['jobcmd']
            else:
                jobcmd = None

            jobtagged = 0

            #print(f"req_fname {req_fname} and filename {filename}")
        else:
            # Not neccessarily so that a job updates filename/job command
            filename = None
            jobcmd = None
            #print(f"No filename/command")


        ## Alter the corresponding job:
        njobs = get_job_len(jobix) 
        print(f"SPECIFYING JOBID= {jobid}")
        out = pmdb.set(db_file_.format(jobix=jobix), njobs,
                jobid=jobid,
                job=jobcmd,
                jobstage=jobstage,
                jobpath=filename,
                jobdatecommitted=jobtime,
                jobtagged=jobtagged)

        print(f"Altered job, with response from pmdb: {out}")

        #job = Engine.query.filter_by(jobix=jobix, jobid=jobid).first()
        #print(f"Found job {job}")
        #if job:
        #    job.jobstage = jobstage
        #    job.filename = filename or job.filename
        #    job.jobcmd = jobcmd or job.jobcmd
        #    job.datecommitted=datetime.datetime.now()
        #    job.flagged = 0

        #    #print(f"Committing changes to job {job}")

        #    # Update entry
        #    db.session.commit()

        #    #print(f"Committed change!")

        #    #print(f"Broadcasting change!")

        #    # Broadcast changes to webpage conncetions
        bcast_change_job(jobid,
                {'jobstage': jobstage,
                 'filename': fn_screen,
                 'datecommitted': jobtime.decode("utf8"),
                 'flagged': jobtagged})
        bcast_jobstage_counter()

            #print(f"Done with broadcasting!")

        return f"Success, job {jobid} was updated to stage {jobstage}!"

    else:
        return json.dumps("busy").encode("utf8")



"""
"
" check_stages
"
"""
@app.route("/check_stages", methods=['GET'])
@app.route("/check_stages/<int:old_bad_stage>/<int:new_redo_stage>", methods=['GET'])
def check_stages(old_bad_stage=None, new_redo_stage=None):
    """ Checks and moves jobs to a lower stage if they are stuck (past lifetime)
        reads jobix from encrypted header
    """
    init()

    if g.isworking:
        return json.dumps("busy").encode("utf8")

    if 'jobix' in request.headers:
        jobix = int(request.headers['jobix'])

        init_db_if_not_existing(jobix)

        db = db_file_.format(jobix=jobix)
        n_jobs = get_job_len(jobix)

        if old_bad_stage and new_redo_stage:
            # Downgrade jobs that are flagged
            n_bad_jobs = 0
            is_good = check_job_status(jobstage=old_bad_stage, jobix=jobix)

            if not is_good:
                print("Some jobs will be downgraded!")
                jobs = pmdb.search(db, n_jobs, 
                        jobstage=('==', old_bad_stage),
                        jobtagged=('==', 1)
                        )

                n_bad_jobs = len(jobs)
                for j in jobs:
                    t_now = datetime.datetime.now().strftime(dateformat)
                    pmdb.set(db, n_jobs, j,
                            jobstage = new_redo_stage,
                            jobtagged = 0,
                            jobdatecommitted=t_now.encode('utf8'),
                            )
                    # Broadcast changes to webpage conncetions
                    bcast_change_job(j.jobid,
                            {'datecommitted': t_now,
                             'jobstage': jobstage,
                             'flagged': 0})
                    bcast_jobstage_counter()


            #    jobs = Engine.query.filter_by(jobstage=old_bad_stage, flagged=1)
            #    for j in jobs:
            #        j.datecommitted=datetime.datetime.now()
            #        j.jobstage = new_redo_stage
            #        j.flagged = 0
            #        n_bad_jobs += 1

            #        # Broadcast changes to webpage conncetions
            #        bcast_change_job(j.jobid,
            #                {'datecommitted': j.datecommitted.strftime(dateformat),
            #                 'jobstage': j.jobstage,
            #                 'flagged': j.flagged})
            #        bcast_jobstage_counter()

            #    # Update these stages
            #    db.session.commit()

            return json.dumps({
                'Are all simulations within time frame?': is_good,
                'n_bad_jobs were degraded:': n_bad_jobs}).encode('utf8')
        else:
            is_good = check_job_status(jobix=jobix)
            return json.dumps({'Are all simulations within time frame?': is_good}).encode('utf8')

    else:
        abort(403, "jobix is missing from headers!")



"""
"
" get_jobs
"
"""
@app.route("/get")
@app.route("/get/<int:jobstage>")
@app.route("/get/<int:jobstage>/<int:nextstage>")
def get_jobs(jobstage=None, nextstage=None):
    """ Returns first available job in `jobstage`
    """
    init()

    if g.isworking:
        return json.dumps("busy").encode("utf8")

    g.njobs = count_jobs()


    if jobstage is not None:
        # Job list index:
        if 'jobix' in request.headers:
            jobix = int(request.headers['jobix'])
        else:
            abort(403, "jobix is missing from headers!")

        init_db_if_not_existing(jobix)
        db = db_file_.format(jobix=jobix)
        n_jobs = g.njobs #get_len_jobs(jobix)

        # Get the first available job with this jobstage
        #job = Engine.query.filter_by(jobstage=jobstage).first()
        job = pmdb.search(db, n_jobs, jobstage=('==',jobstage), only_first=True)
        # Make it unavailable for other searches right away
        print(job, "WAS FOUND searching amonb n_jobs:", n_jobs, "for", jobstage)
        if len(job) > 0:
            #print(f"SETTING VALUE (TAKEN) out of {n_jobs}, jobid {job[0]}, jobstage {int(jobstage+1)}")
            pmdb.set(db,
                    n_jobs, 
                    jobid=job[0],
                    job=None,
                    jobstage=int(jobstage+1),
                    jobpath=None,
                    jobdatecommitted=None,
                    jobtagged=0,
                    )

            print("GETTING VALUES (INFO) ")
            info = pmdb.get(db, n_jobs, job[0])

            # Goes to worker:
            print("RETURNS VALUES (TO WORKER) ")
            res = json.dumps({'jobid': info[0],
                              'job': info[1].decode('utf8'),
                              'jobstage': info[2],
                              'jobix': jobix}).encode("utf8")

            # Broadcast changes to webpage conncetions
            bcast_change_job(job[0],
                    {'jobstage': jobstage+1,
                     'flagged': 0})
            # And percentage/progress bar
            bcast_jobstage_counter()

        #if job is not None:
        #    res = json.dumps({'jobid': job.jobid,
        #                      'jobcmd': job.jobcmd,
        #                      'jobstage': job.jobstage,
        #                      'jobix': job.jobix}).encode("utf8")
        #    if nextstage:
        #       # Immediately make this job unavailable!
        #       job.jobstage = nextstage
        #    else:
        #       job.jobstage += 1

        #    # No longer flagged as bad
        #    job.flagged = 0

        #    # Update the time for comitting at this stage
        #    job.datecommitted=datetime.datetime.now()

        ##    db.session.commit()

        #    # Broadcast changes to webpage conncetions
        #    bcast_change_job(job.jobid,
        #            {'jobstage': job.jobstage,
        #             'flagged': job.flagged})
        #    # And percentage/progress bar
        #    bcast_jobstage_counter()

        else:
            res = json.dumps('nada').encode("utf8")

        return res
    else:
        # Display web page with all jobs
        # or, if there are too many, show only jobs in odd stages
        # where they are performing work (assuming even stages are init/final ones)
        njobs = count_jobs()
        nstages = count_stages()
        ncomplete = count_jobs(nstages)

        if len(glob.glob("*.pmem")) > 0:
            jobix_ = int(glob.glob(db_file_.replace("{jobix}","*"))[0].split("_")[1].split(".")[0])
            db_ = db_file_.format(jobix=jobix_)
        else:
            db_ = None

        # set some global variables
        g.njobs = njobs
        g.nstages = nstages
        g.ncomplete = ncomplete
        g.db = db_

        alljobs = []

        if njobs > 1024:
            # Too many jobs!
            #noddstages = nstages//2 + nstages%2
            #for i in range(noddstages):
            #    iodd = 2*i + 1
            #    #odd_jobs = Engine.query.filter_by(jobstage=iodd)
            #    odd_jobs = pmdb.search(db_, njobs,
            #            jobstage=("==", iodd),
            #            )
            #    # add these indices to the web page list
            #    alljobs.extend(odd_jobs)
            # Add 100 representative jobs
            alljobs = range(0,njobs,njobs//100)

        else:
            #alljobs = Engine.query.all()
            alljobs = range(njobs)

        for job in alljobs:
            #jobs_on_webpage.append(job.jobid)
            if job not in jobs_on_webpage:
                jobs_on_webpage.append(job)
        
        # From the indices, get the jobs, returned like this:
        # [0, b'[12, 34]', 8, b'a_special_path!', b'2018-05-02 18:11:12', 0]

        jobdictlist = []
        for job in alljobs:
            res = pmdb.get(g.db, g.njobs, job)
            jobdictlist.append( {
                'jobid': res[0],
                'jobstage': res[2],
                'filename': res[3],
                'flagged': res[5],
                'datecommitted': res[4].decode('utf8'),
                })



        check_job_status()
        jobcount = bcast_jobstage_counter()
        return render_template('joblist.html',
                #jobs=alljobs,
                jobs=jobdictlist,
                njobs=njobs, ncomplete=ncomplete, nstages=nstages,
                jobcount=jobcount)

"""
"
" set_lifetime
"
"""
@app.route("/lifetime")
@app.route("/lifetime/<int:jobstage>")
@app.route("/lifetime/<int:jobstage>/<int:n_minutes>")
def set_lifetime(jobstage=None, n_minutes=None):
    """ Sets the lifetime a job can have in a jobstage
    """

    init()

    if g.isworking:
        return json.dumps("busy").encode("utf8")

    if jobstage and n_minutes:
        # Set a lifetime
        # Job list index:
        if 'jobix' in request.headers:
            jobix = int(request.headers['jobix'])
        else:
            abort(403, "jobix is missing from headers!")

        init_db_if_not_existing(jobix)

        # Check if they already have been set
        #res = Lifetime.query.filter_by(jobstage=jobstage).first()
        #if not res:
        #    entry = Lifetime(
        #            jobix = jobix,
        #            jobstage = jobstage,
        #            lifetime = n_minutes
        #            )
        #else:
        #    res.lifetime = datetime.time(0,int(lifetime))
        #db.session.commit()
        update_lifetimes(jobix, jobstage, n_minutes)


    elif jobstage:
        # Get lifetime for this job stage
        #res = Lifetime.query.filter_by(jobstage=jobstage).first()
        if len(glob.glob("*.pmem")) > 0:
            jobix_ = int(glob.glob(db_file_.replace("{jobix}","*"))[0].split("_")[1].split(".")[0])
            res = get_lifetimes(jobix_)
        else:
            res = []
        return res[0]
    else:
        # Return HTML page showing current lifetimes
        #alltimes = Lifetime.query.all()
        if len(glob.glob("*.pmem")) > 0:
            jobix_ = int(glob.glob(db_file_.replace("{jobix}","*"))[0].split("_")[1].split(".")[0])
            lifetimes = get_lifetimes(jobix_)
        else:
            lifetimes = [] 
        out = [{'jobstage':i, 'lifetime':lifetimes[i]} for i in range(len(lifetimes))]

        return render_template('lifetimes.html', lifetimes=out)

"""
"
" count_jobs
"
"""
def count_jobs(jobstage=-1):
    """ Counts number of jobs with
        a given jobstage or them all
    """

    #print("IN COUNT_JOBS")

    # ask db:

    njobs = 0
    if len(glob.glob("*.pmem")) > 0:
        jobix_ = int(glob.glob(db_file_.replace("{jobix}","*"))[0].split("_")[1].split(".")[0])
        db = db_file_.format(jobix=jobix_)

        if jobstage == -1:
            njobs = pmdb.count(db)[0]
            g.njobs = njobs
        else:
            #print(f"COUNTING JOBS IN STAGE {jobstage} ")
            njobs = pmdb.count(db)[0]
            g.njobs = njobs
            jobs = pmdb.search(db, njobs, jobstage=('==', jobstage))
            njobs = len(jobs)
            #print(f"Found {jobs} with len {njobs}")

    #print("DONE IN COUNT_JOBS")

    ## Will have to do a hack:
    #if len(glob.glob("*.pmem")) > 0:
    #    jobix_ = int(glob.glob(db_file_.replace("{jobix}","*"))[0].split("_")[1].split(".")[0])
    #    db = db_file_.format(jobix=jobix_)

    #    n_jobs = get_job_len(jobix_)

    #    print(f" WE HAVE n_jobs= {n_jobs} and are going to look for more AT STAGE {jobstage} ")

    #    if jobstage and n_jobs:
    #        valid_jobs = pmdb.search(db, n_jobs, jobstage=('==', jobstage))
    #        n_jobs = len(valid_jobs)
    #else :
    #    n_jobs = 0
        

    print(f"Found n jobs: {njobs} with jobstage = {jobstage} ")

    return njobs

    #if jobstage is not None:
    #    extra = f"WHERE jobstage = {jobstage};"
    #else:
    #    extra = ";"
    #query = db.engine.execute(f"""
    #    SELECT count(*)
    #    FROM engine
    #    {extra}
    #    """).first()
    #print(f"Found n_jobs: {query[0]} with jobstage: {jobstage}")
    #return int(query[0])

"""
"
" count_stages
"
"""
def count_stages():
    """ Counts number of job stages
    """
    #lifetimes = Lifetime.query.all()
    #max_stage = -1

    #for lt in lifetimes:
    #    max_stage = lt.jobstage if lt.jobstage > max_stage else max_stage

    # Will have to do a hack:
    if len(glob.glob("*.pmem")) > 0:
        jobix_ = int(glob.glob(db_file_.replace("{jobix}","*"))[0].split("_")[1].split(".")[0])
        lifetimes = get_lifetimes(jobix_)
    else :
        lifetimes = []

    max_stage = len(lifetimes)

    print(f"Found max_stage: {max_stage}")
    return max_stage




"""
"
" check_job_status
"
"""
def check_job_status(jobstage=None, jobix=None):
    isgood = 1
    # Return status code of jobs
    #jobs = Engine.query.filter_by(jobstage=jobstage)

    db = db_file_.format(jobix=jobix),
    n_jobs = get_job_len(jobix),


    if jobstage:
        jobs = pmdb.search(
                db,
                n_jobs,
                jobstage=("==", jobstage) if jobstage else None,
                #jobtagged=("==", 1),
                )
    else:
        # No need to do a potentially heavy search
        jobs = range(get_job_len(jobix))

    t_now = datetime.datetime.now()
    lifetimes = get_lifetimes(jobix)

    for j in jobs:
        #j_stage = j.jobstage
        #t_then = j.datecommitted
        #if jobix:
        #    q = Lifetime.query.filter_by(jobix=jobix, jobstage=j_stage).first()
        #else:
        #    q = Lifetime.query.filter_by(jobstage=j_stage).first()
        ##print("RESULT:", q, 'for jobstage', j_stage)
        #if q and q.lifetime:
        #    if t_now - t_then > datetime.timedelta(minutes=q.lifetime):
        #        # Turn into negative
        #        isgood *= 0
        #        j.flagged = 1
        #else:
        #    isgood += 100
        #    #print(f"WARNING! No maximum time has been set for stage {j_stage}!")
        # Check if time stamps and lifetimes work out
        job_data = pmdb.get(db, n_jobs, j)
        j_stage = job_data[2]
        t_then = datetime.datetime.strptime(job_data[4], dateformat) 
        if t_now - t_then > datetime.timedelta(minutes=lifetimes[j_stage]):
            isgood *= 0
            # Update value of job
            pmdb.set(db, n_jobs, j, 
                    jobtagged=1)
        
    # Update flags
    # db.session.commit()

    return isgood

"""
"
" init_db_if_not_existing
"
"""

def init_db_if_not_existing(jobix):
    #jlist = 'joblist.db'
    #lives = 'lifetimes.db'
    jlist = db_file_.format(jobix=jobix)
    lives = lt_file_.format(jobix=jobix)

    print("Attempting to connect to db file...")
    status = pmdb.init_pmdb(jlist)
    print("Initialized or connected to database")
    print(f"N existing elements: {status[0]} and init message: {status[1]}")

    #if not os.path.exists(jlist) or os.stat(jlist).st_size ==0:
    #    # Initialize DB
    #    #from . import db
    #    if os.path.exists(jlist): os.remove(jlist)
    #    db.create_all()
    #else:
    #    # Check if all desired columns exist
    #    pass

    #if not os.path.exists(lives) or os.stat(lives).st_size ==0:
    #    # initialize lifetime DB
    #    if os.path.exists(lives): os.remove(lives)
    #    db.create_all(bind='lifetimes')

    return 'ok'

"""
"
" update_webpage_jobstage_counter
"
"""
def bcast_jobstage_counter():
    """ Counts number of jobs in each of
        the stages and sends this to the
        client
    """
    try:
        g.nstages
    except AttributeError:
        g.nstages = count_stages()
    try:
        g.njobs
    except AttributeError:
        g.njobs = count_jobs()

    stagejobs = dict()
    stagejobs['ntot'] = g.njobs
    stagejobs['nstages'] = g.nstages

    # Enumerate through each stage and find the corresponding number of jobs
    stagejobs['count'] = [count_jobs(i) for i in range(g.nstages + 1)]


    # Broadcast this newly obtained list
    socketio.emit('counted jobs in stages', stagejobs, broadcast=True)
    #socketio.emit('counted jobs in stages', {'empty': 0}, broadcast=True)

    return stagejobs['count']


"""
"
" change_job
"
"""
def bcast_change_job(jobid=None, jobkw=None):
    """ Broadcasts changes to this job
        to all connected clients
        which will update the relevant tags
    """
    #print("IN BCAST_CHANGE_JOB")
    if jobkw:
        #print("FOUND KW")
        if jobid in jobs_on_webpage:
            data_out = {'jobid':jobid, **jobkw}
            #data_out = json.dumps(data_out_dict).encode('utf8')
            socketio.emit('job change', data_out, broadcast=True)

    return

def bcast_reload():
    """ Broadcasts a reload request
    """
    socketio.emit('reload', broadcast=True)




if __name__ == "__main__":

    #app.run(ssl_context='adhoc')
    socketio.run(app) #, ssl_context='adhoc')

