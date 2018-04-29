# job_manager
Flask-based supercomputing job manager

![Some workers are flagged as inactive/died as they have spent more time in their stage than allowed](../master/img/screenshot.png)

## Description

This [Flask](http://flask.pocoo.org/) web application manages a list of jobs that can be pushed from a master node and obtained from an arbitrary number of independent worker nodes that not necessarily have the ability to communicate between themselves.

Examples of this can be:
- A supercomputer where the clusters have upper limits on the number of nodes that can be organized as a single job under MPI. One needs very clever scheduling and job assignments to make sure that **(a)** all jobs get done, **(b)** no overlap between workers occur, and **(c)** corrupted results do not get forwarded in a data processing pipeline.
- Several independent nodes that will work on an unknown amount of data as it become available at an unknown time.
- A project where MPI/communication between workers is overkill as the problem is massively parallelizable.

It features a Flask interface allowing for JSON requests in Python from workers, storing of job information in an [SQLite](https://www.sqlite.org/index.html) database, and also provides a web interface showing the overall progress using [Socket.io](https://socket.io/).

## Setup

Clone the repository to a folder where it could potentially be launched as a WSGI application, or locally as a Flask application.

For Apache, the WSGI setup requires that the following options to be specified in the configuration file (`.conf`):

```ApacheConf
    WSGIDaemonProcess job_man_app user=job_man_user group=job_man_group threads=5
    WSGIScriptAlias / /path/to/job_manager/job_manager_app.wsgi

    <Directory /path/to/job_manager>
        WSGIProcessGroup job_man_app
        WSGIApplicationGroup %{GLOBAL}
        Require all granted
    </Directory>
```

where the `job_manager_app.wsgi` could look like this:

```python
import sys
sys.path.insert(0, '/path/to/job_manager')
from jobserver import app as application
```

Make sure that [WSGI is built for the Python 3.6 version on the system](http://modwsgi.readthedocs.io/en/develop/user-guides/quick-installation-guide.html). Install `mod24_wsgi-python36` using the package manager and `pip-3.6`, for example.

Other dependencies for the server:

- json
- zlib
- flask
- flask-SQLAlchemy
- flask-SocketIO

whereas the client needs:

- json
- ssl (potentially, if HTTPS is desired)
- urllib (beware of scam versions phoning home!)

## Usage

### Listening structure

The server will listen for calls under:

- `/`
- `/init`
- `/set`
- `/update_job`
- `/check_stages`
- `/get` -- will also serve a web page showing jobs and their progress
- `/lifetime` -- will also serve a web page showing the life times in minutes for the different job stages

### Example

A Python function fetching an available job ID at a stage `job_stage` from the job manager listening at `{url}`:

```python
import urllib.request as u
authead = {'jobix': my_secret_numeric_id}

def ask_for_job(job_stage):
    req = u.Request(f"{url}/get/{job_stage}", headers=authead)
    res = u.urlopen(req, context=context)
    info = json.loads(res.read().decode('utf8'))
    res.close()
    print('info', info)

    return info
```

which then can be processed by the local worker before a reply is returned with a function that could look like this:

```python
def update_job(jobid, next_stage):
    req = u.Request(f"{url}/update_job/{jobid}/{next_stage}", headers=authead)
    res = u.urlopen(req, context=context)
    # Should verify that the job was accepted
    print(res.read())
    res.close()
```
**Important**: `next_stage` must be **two** levels higher than the stage you obtain the job from, as the job immediately is flagged as *working* by incrementing its stage by one when it is fetched by using `/get/{job_stage}`.

This way is a job either in two possible states:

1. Even job stages: available for processing or done
2. Odd jobb stages: being done work on.



## Examples

See

- [`call.py`](../master/call.py) for possible function calls to a web server or
- [`call2.py`](../master/call2.py) for a functional approach for a worker that does work until no more remains.

