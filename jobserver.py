
import json
import zlib
import os
import datetime

from flask import Flask, request, g
from flask import render_template
from flask import abort
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO


app = Flask(__name__, instance_relative_config=True)
socketio = SocketIO(app)

# Job list SQLite database
app.config['SQLALCHEMY_DATABASE_URI'] = "sqlite:///joblist.db"
app.config['SQLALCHEMY_BINDS'] = {
        'lifetimes': "sqlite:///lifetimes.db"
        }
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

dateformat = "%Y-%m-%d %H:%M:%S"

# List of jobs that have been pushed to web page
jobs_on_webpage = []

db = SQLAlchemy(app)


class Engine(db.Model):
    # Authentication needed to look up entries
    jobix = db.Column(db.Integer)
    # Job IDs
    jobid = db.Column(db.Integer, primary_key=True)
    # Jobs, this is the instruction
    jobcmd = db.Column(db.PickleType)
    # Job stage: user can request jobs to be moved between stages
    jobstage = db.Column(db.Integer)
    # Job commitment time: a job could die, meaning it should be degraded
    # and made available for workers
    datecommitted = db.Column(db.DateTime)
    # If the job has surpassed its allocated time:
    flagged = db.Column(db.Integer)
    # Filnames for last output
    filename = db.Column(db.String)

class Lifetime(db.Model):
    __bind_key__ = 'lifetimes'
    # Combines: jobix, jobstage and lifetime
    id = db.Column(db.Integer, autoincrement=True, primary_key=True)
    jobix = db.Column(db.Integer)
    jobstage = db.Column(db.Integer)
    lifetime = db.Column(db.Integer) # minutes



"""
"
" get_jobs
"
"""
@app.route("/")
def hello():
    # Initialize files if they don't exist
    # and have data
    init_db_if_not_existing()
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
    if 'jobix' in request.headers:
        jobix = int(request.headers['jobix'])
    else:
        abort(403, "jobix is missing from headers!")

    if stage_times:
        lifetimes = stage_times.split('/')
        n_stages = len(lifetimes)

        # Verify that the lifetimes haven't already been set
        # (they can be changed with another function)
        if not Lifetime.query.filter_by(jobix=jobix).first():
            # add these to the database
            for i, lt in enumerate(lifetimes):
                lt = int(lt)
                entry = Lifetime(
                        jobix = jobix,
                        jobstage = i,
                        lifetime = lt
                        )
                db.session.add(entry)
                db.session.commit()
        lts_q = Lifetime.query.all()
        lts_new = {i.jobstage: i.lifetime for i in lts_q}
        # Reload web pages
        bcast_reload()
        bcast_jobstage_counter()
        return json.dumps(lts_new).encode("utf8")
    else:
        # Reload web pages
        bcast_reload()
        bcast_jobstage_counter()
        return json.dumps(Lifetime.query.all()).encode("utf8")

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
    init_db_if_not_existing()

    if request.method == 'POST':
        jobix = int(request.headers['jobix'])
        ids = json.loads(zlib.decompress(request.data))
        # Create new DB entries:
        for ii in ids.keys():
            #print(f"checking: jobix {jobix} and jobid {ii}")
            #print("Does it exist?", Engine.query.filter_by(jobix=jobix, jobid=ii).first())
            if not Engine.query.filter_by(jobix=jobix, jobid=ii).first():
                # This job does not exist:
                entry = Engine(
                        jobix=jobix,
                        jobid=ii,
                        jobcmd=ids[ii]['jobcmd'],
                        jobstage=ids[ii]['jobstage'],
                        datecommitted=datetime.datetime.now())
                db.session.add(entry)
                db.session.commit()
        bcast_reload()
        bcast_jobstage_counter()

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

    init_db_if_not_existing()

    #print("Updated DB")

    if request.method == 'POST':
        # We could receive both a save path and a new unpickleable job command
        # that the worker for the next step can use for its
        # analysis
        req = json.loads(zlib.decompress(request.data))
        if 'filename' in req:
            filename = req['filename'].encode("utf8")
        else:
            filenae = None
        if 'jobcmd' in req:
            jobcmd = req['jobcmd']
        else:
            jobcmd = None

        #print(f"req_fname {req_fname} and filename {filename}")
    else:
        # Not neccessarily so that a job updates filename/job command
        filename = None
        jobcmd = None
        #print(f"No filename/command")

    # Alter the corresponding job:
    job = Engine.query.filter_by(jobix=jobix, jobid=jobid).first()
    print(f"Found job {job}")
    if job:
        job.jobstage = jobstage
        job.filename = filename or job.filename
        job.jobcmd = jobcmd or job.jobcmd
        job.datecommitted=datetime.datetime.now()
        job.flagged = 0

        #print(f"Committing changes to job {job}")

        # Update entry
        db.session.commit()

        #print(f"Committed change!")

        #print(f"Broadcasting change!")

        # Broadcast changes to webpage conncetions
        bcast_change_job(job.jobid,
                {'jobstage': job.jobstage,
                 'filename': job.filename,
                 'datecommitted': job.datecommitted.strftime(dateformat),
                 'flagged': job.flagged})
        bcast_jobstage_counter()

        #print(f"Done with broadcasting!")

    return f"Success, job {jobid} was updated to stage {jobstage}!"



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
    if 'jobix' in request.headers:
        jobix = int(request.headers['jobix'])

        init_db_if_not_existing()

        if old_bad_stage and new_redo_stage:
            # Downgrade jobs that are flagged
            n_bad_jobs = 0
            is_good = check_job_status(jobstage=old_bad_stage, jobix=jobix)
            if not is_good:
                print("Some jobs will be downgraded!")
                jobs = Engine.query.filter_by(jobstage=old_bad_stage, flagged=1)
                for j in jobs:
                    j.datecommitted=datetime.datetime.now()
                    j.jobstage = new_redo_stage
                    j.flagged = 0
                    n_bad_jobs += 1

                    # Broadcast changes to webpage conncetions
                    bcast_change_job(j.jobid,
                            {'datecommitted': j.datecommitted.strftime(dateformat),
                             'jobstage': j.jobstage,
                             'flagged': j.flagged})
                    bcast_jobstage_counter()

                # Update these stages
                db.session.commit()

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

    init_db_if_not_existing()

    if jobstage is not None:
        # Job list index:
        jobix = int(request.headers['jobix'])
        # Get the first available job with this jobstage
        job = Engine.query.filter_by(jobstage=jobstage).first()
        if job is not None:
            res = json.dumps({'jobid': job.jobid,
                              'jobcmd': job.jobcmd,
                              'jobstage': job.jobstage,
                              'jobix': job.jobix}).encode("utf8")
            if nextstage:
               # Immediately make this job unavailable!
               job.jobstage = nextstage
            else:
               job.jobstage += 1

            # No longer flagged as bad
            job.flagged = 0

            # Update the time for comitting at this stage
            job.datecommitted=datetime.datetime.now()

            db.session.commit()

            # Broadcast changes to webpage conncetions
            bcast_change_job(job.jobid,
                    {'jobstage': job.jobstage,
                     'flagged': job.flagged})
            # And percentage/progress bar
            bcast_jobstage_counter()

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

        # set some global variables
        g.njobs = njobs
        g.nstages = nstages
        g.ncomplete = ncomplete

        alljobs = []

        if njobs > 1024:
            # Too many jobs!
            noddstages = nstages//2 + nstages%2
            for i in range(noddstages):
                iodd = 2*i + 1
                odd_jobs = Engine.query.filter_by(jobstage=iodd)
                # add these to the web page list
                alljobs.extend(odd_jobs)
        else:
            alljobs = Engine.query.all()

        # Find their indices:
        for job in alljobs:
            jobs_on_webpage.append(job.jobid)

        check_job_status()
        jobcount = bcast_jobstage_counter()
        return render_template('joblist.html',
                jobs=alljobs,
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

    init_db_if_not_existing()

    if jobstage and n_minutes:
        # Set a lifetime
        jobix = int(request.headers['jobix'])
        # Check if they already have been set
        res = Lifetime.query.filter_by(jobstage=jobstage).first()
        if not res:
            entry = Lifetime(
                    jobix = jobix,
                    jobstage = jobstage,
                    lifetime = n_minutes
                    )
        else:
            res.lifetime = datetime.time(0,int(lifetime))
        db.session.commit()

    elif jobstage:
        # Get lifetime for this job stage
        res = Lifetime.query.filter_by(jobstage=jobstage).first()
        return res
    else:
        # Return HTML page showing current lifetimes
        alltimes = Lifetime.query.all()
        return render_template('lifetimes.html', lifetimes=alltimes)

"""
"
" count_jobs
"
"""
def count_jobs(jobstage=None):
    """ Counts number of jobs with
        a given jobstage or them all
    """

    if jobstage is not None:
        extra = f"WHERE jobstage = {jobstage};"
    else:
        extra = ";"
    query = db.engine.execute(f"""
        SELECT count(*)
        FROM engine
        {extra}
        """).first()
    print(f"Found n_jobs: {query[0]} with jobstage: {jobstage}")
    return int(query[0])

"""
"
" count_stages
"
"""
def count_stages():
    """ Counts number of job stages
    """
    lifetimes = Lifetime.query.all()
    max_stage = -1

    for lt in lifetimes:
        max_stage = lt.jobstage if lt.jobstage > max_stage else max_stage

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
    if jobstage:
        jobs = Engine.query.filter_by(jobstage=jobstage)
    else:
        jobs = Engine.query.all()

    t_now = datetime.datetime.now()
    for j in jobs:
        j_stage = j.jobstage
        t_then = j.datecommitted
        if jobix:
            q = Lifetime.query.filter_by(jobix=jobix, jobstage=j_stage).first()
        else:
            q = Lifetime.query.filter_by(jobstage=j_stage).first()
        #print("RESULT:", q, 'for jobstage', j_stage)
        if q and q.lifetime:
            if t_now - t_then > datetime.timedelta(minutes=q.lifetime):
                # Turn into negative
                isgood *= 0
                j.flagged = 1
        else:
            isgood += 100
            #print(f"WARNING! No maximum time has been set for stage {j_stage}!")
    # Update flags
    db.session.commit()

    return isgood

"""
"
" init_db_if_not_existing
"
"""

def init_db_if_not_existing():
    jlist = 'joblist.db'
    lives = 'lifetimes.db'
    if not os.path.exists(jlist) or os.stat(jlist).st_size ==0:
        # Initialize DB
        #from . import db
        if os.path.exists(jlist): os.remove(jlist)
        db.create_all()
    else:
        # Check if all desired columns exist
        pass

    if not os.path.exists(lives) or os.stat(lives).st_size ==0:
        # initialize lifetime DB
        if os.path.exists(lives): os.remove(lives)
        db.create_all(bind='lifetimes')

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
    if jobkw:
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

