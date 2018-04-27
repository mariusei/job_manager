
import json
import os
from flask import Flask, request, g
from flask import render_template
#from flask import abort, Response
from werkzeug.exceptions import abort
from flask_sqlalchemy import SQLAlchemy
import datetime


app = Flask(__name__, instance_relative_config=True)

# Job list SQLite database
app.config['SQLALCHEMY_DATABASE_URI'] = "sqlite:///joblist.db"
app.config['SQLALCHEMY_BINDS'] = {
        'lifetimes': "sqlite:///lifetimes.db"
        }
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


class Engine(db.Model):
    # Authentication needed to look up entries 
    jobix = db.Column(db.Integer)
    # Job IDs pointing to position in local job array
    jobid = db.Column(db.Integer, primary_key=True)
    # Job stage: user can request jobs to be moved between stages
    jobstage = db.Column(db.Integer)
    # Job commitment time: a job could die, meaning it should be degraded 
    # and made available for workers
    datecommitted = db.Column(db.DateTime)
    # If the job has surpassed its allocated time:
    flagged = db.Column(db.Integer)
    # Filnames for last output
    filename = db.Column(db.LargeBinary)

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
        return json.dumps(lts_new).encode("utf8")
    else:
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
    
    if request.method == 'POST':
        jobix = int(request.headers['jobix'])
        ids = request.get_json()
        # Create new DB entries:
        for ii in ids.keys():
            #print(f"checking: jobix {jobix} and jobid {ii}")
            #print("Does it exist?", Engine.query.filter_by(jobix=jobix, jobid=ii).first())
            if not Engine.query.filter_by(jobix=jobix, jobid=ii).first():
                # This job does not exist:
                entry = Engine(
                        jobix=jobix,
                        jobid=ii,
                        jobstage=ids[ii]['jobstage'],
                        datecommitted=datetime.datetime.now())
                db.session.add(entry)
                db.session.commit()
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

    if request.method == 'POST':
        # We will receive a save path
        # that the worker for the next step can use for its
        # analysis
        req_fname = request.get_json()
        filename = bytes(req_fname['filename'].encode("utf8"))
    else:
        # Not neccessarily so that a job updates a filename
        filename = None

    # Alter the corresponding job:
    job = Engine.query.filter_by(jobix=jobix, jobid=jobid).first()
    if job:
        job.jobstage = jobstage
        job.filename = filename or job.filename
        job.datecommitted=datetime.datetime.now()
        job.flag = 0

        # Update entry
        db.session.commit()

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

    if jobstage is not None:
        # Job list index:
        jobix = int(request.headers['jobix'])
        # Get the first available job with this jobstage
        job = Engine.query.filter_by(jobstage=jobstage).first()
        if job is not None:
            res = json.dumps({'jobid': job.jobid,
                              'jobstage': job.jobstage,
                              'jobix': job.jobix}).encode("utf8")
            if nextstage:
               # Immediately make this job unavailable!
               job.jobstage = nextstage
               db.session.commit()
        else:
            res = json.dumps('nada').encode("utf8")
        return res
    else:
        # Display web page with all jobs
        njobs = count_jobs()
        nstages = count_stages()
        ncomplete = count_jobs(nstages)
        if njobs > 0:
            percent = int(ncomplete/njobs * 100)
        else:
            percent = 0

        alljobs = Engine.query.all()
        check_job_status()
        return render_template('joblist.html', jobs=alljobs, percent=percent)

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

    if jobstage:
        extra = f"WHERE jobstage = {jobstage};"
    else:
        extra = ";"
    query = db.engine.execute(f"""
        SELECT count(*)
        FROM engine
        {extra}
        """).first()
    print(f"Found n_jobs: {query[0]}")
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
        print("RESULT:", q, 'for jobstage', j_stage)
        if q and q.lifetime:
            if t_now - t_then > datetime.timedelta(minutes=q.lifetime):
                # Turn into negative
                isgood *= 0
                j.flagged = 1
        else:
            isgood += 100
            print(f"WARNING! No maximum time has been set for stage {j_stage}!")
    # Update flags
    db.session.commit()

    return isgood



if __name__ == "__main__":
    if not os.path.exists('joblist.db'):
        # Initialize DB
        #from . import db
        db.create_all()
    if not os.path.exists('lifetimes.db'):
        # initialize lifetime DB
        db.create_all(bind='lifetimes')

    app.run(ssl_context='adhoc')

