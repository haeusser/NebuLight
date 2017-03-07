#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK

"""
NebuLight: A super light weight batch processor for arbitrary command line commands.
(c) 2017 Philip Haeusser, haeusser@cs.tum.edu

This library facilitates batch processing of a list of command line commands.

Example usage:
./nebulight.py add "echo 'OK' >> results.log"
./nebulight.py status
./nebulight.py start
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import datetime
import os
import sqlite3 as sql
import time
import subprocess
import shlex
import sys
import argcomplete

# Constants.
QUEUED = 'queued'
PROCESSING = 'processing'
DONE = 'done'
FAILED = 'failed'
IDLE_CHECK_INTERVAL_MIN = 0.1


def _add_single_job(cursor, cmd, status):
    cursor.execute("insert into jobs(cmd, status, tries) values (?, ?, ?)", (cmd, status, 0))


def _get_or_create_db(db_name):
    conn = sql.connect(db_name)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS jobs (job_id INTEGER PRIMARY KEY, cmd, status, tries);''')
    return conn, c


def _commit_and_close(conn, cursor):
    conn.commit()
    cursor.close()
    conn.close()


def _print_not_implemented():
    print("Sorry, not implemented yet :-( Contributions are welcome!")


def _check_for_queued_jobs(db_name):
    conn, c = _get_or_create_db(db_name)
    c.execute('SELECT * FROM jobs WHERE status=?', (QUEUED,))
    rows = c.fetchall()
    _commit_and_close(conn, c)
    return len(rows)


def _pull_and_process(args):
    conn, c = _get_or_create_db(args.db_name)
    c.execute('SELECT * FROM jobs WHERE status=?', (QUEUED,))
    try:
        (id, cmd, stat, tries) = c.fetchone()
    except Exception as e:
        print("Couldn't pull any new jobs." + e.message)
        _commit_and_close(conn, c)
        return

    if tries >= args.max_failures:
        print("This job has failed.")
        conn, c = _get_or_create_db(args.db_name)
        c.execute("UPDATE jobs SET status=? WHERE job_id=?", (FAILED, id))
        _commit_and_close(conn, c)
        return

    c.execute('SELECT * FROM jobs WHERE status=?', (QUEUED,))
    c.execute("UPDATE jobs SET status=?, tries=? WHERE job_id=?", (PROCESSING, tries + 1, id))
    _commit_and_close(conn, c)

    print("Try {}/{} of job #{}: {}".format(tries + 1, args.max_failures, id, cmd))
    if args.gpu is not None:
        _set_gpu(args)

    rc = 1
    try:
        proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        while True:
            output = proc.stdout.readline()
            if output == '' and proc.poll() is not None:
                break
            if output:
                print('NL>> ' + output.strip())
        rc = proc.poll()

        if rc == 0:
            conn, c = _get_or_create_db(args.db_name)
            c.execute("UPDATE jobs SET status=? WHERE job_id=?", (DONE, id))
            _commit_and_close(conn, c)
            print('Job done. Process ended with return code', rc)
            return
    except OSError as e:
        print(e)

    print('Job failed. Process ended with return code', rc)
    conn, c = _get_or_create_db(args.db_name)
    c.execute("UPDATE jobs SET status=? WHERE job_id=?", (QUEUED, id))
    _commit_and_close(conn, c)


def add(args):
    """
    Adds one job from the command line.
    :param args: An argparse object containing the following properties:
            job: A string containing a command line command.
            db_name: A string containing the database filename.
    :return: Nothing
    """
    job = args.job
    print('Adding', job)
    conn, c = _get_or_create_db(args.db_name)
    _add_single_job(c, job, QUEUED)
    _commit_and_close(conn, c)


def add_list(args):
    """
    Adds a number of jobs from an external text file. The file must contain one command per line.
    :param args: An argparse object containing the following properties:
            joblist: A string containing a valid path to a text file.
            db_name: A string containing a filename for the database.
    :return: Nothing.
    """
    joblist = args.joblist
    print('Adding jobs from', joblist)

    assert os.path.exists(joblist), "Joblist file not found: " + joblist

    with open(joblist) as f:
        lines = f.readlines()

    assert len(lines) > 0, "No commands found."

    conn, c = _get_or_create_db(args.db_name)
    for job in lines:
        job = job.rstrip('\n')
        _add_single_job(c, job, QUEUED)
    _commit_and_close(conn, c)

    print("Added", len(lines), "jobs.")


def status(args):
    """
    Prints the current status of the database.
    :param args: An argparse object containing the following properties:
            db_name: A string containing a filename for the database.
    :return: Nothing.
    """
    MAX_LEN_JOBNAME = 40

    if not os.path.exists(args.db_name):
        print("No job queue. Start by adding jobs.")
        return

    conn, c = _get_or_create_db(args.db_name)

    c.execute('SELECT * FROM jobs ORDER BY status')
    rows = c.fetchall()

    spacing = min(max(len(x[1]) for x in rows) + 5, MAX_LEN_JOBNAME + 4)

    num_queued = sum(1 for (_, _, stat, _) in rows if stat == QUEUED)
    num_processing = sum(1 for (_, _, stat, _) in rows if stat == PROCESSING)
    num_done = sum(1 for (_, _, stat, _) in rows if stat == DONE)
    num_failed = sum(1 for (_, _, stat, _) in rows if stat == FAILED)

    str_template = "{:<5}{:<" + str(spacing) + "}{:<15}{}"

    print()
    print(str_template.format("ID", "COMMAND", "STATUS", "TRIES"))
    print("-" * (spacing + 26))

    for row in rows:
        (id, cmd, stat, tries) = row
        cmd = (cmd[:MAX_LEN_JOBNAME] + '...') if len(cmd) > MAX_LEN_JOBNAME else cmd
        print(str_template.format(id, cmd, stat, tries))
    print("-" * (spacing + 26))
    print("{:<3} queued".format(num_queued))
    print("{:<3} processing".format(num_processing))
    print("{:<3} done".format(num_done))
    print("{:<3} failed".format(num_failed))
    print()

    _commit_and_close(conn, c)


def start(args):
    """
    Start the processing loop.
    :param args: An argparse object containing the following properties:
            db_name: A string containing a filename for the database.
            max_idle_minutes: Number of minutes to idle before quitting the processing loop.
    :return: Nothing.
    """
    assert os.path.exists(args.db_name), "No joblist found in {}. Please start with adding jobs.".format(args.db_name)

    num_queued = _check_for_queued_jobs(args.db_name)
    begin_idle_time = datetime.datetime.now()
    end_idle_time = begin_idle_time + datetime.timedelta(seconds=args.max_idle_minutes * 60)

    while num_queued > 0 or datetime.datetime.now() < end_idle_time:
        if num_queued > 0:
            _pull_and_process(args)
            begin_idle_time = datetime.datetime.now()
            end_idle_time = begin_idle_time + datetime.timedelta(seconds=args.max_idle_minutes * 60)
        else:
            str_end_time = end_idle_time.strftime("%H:%M")
            str_delta = str(end_idle_time - datetime.datetime.now())[:-7]
            print("No jobs queued. Waiting for new ones until {} ({} left).".format(str_end_time, str_delta))
            time.sleep(IDLE_CHECK_INTERVAL_MIN * 60)
        num_queued = _check_for_queued_jobs(args.db_name)


def reset(args):
    """
    Set the status of all jobs in the database to 'queued'.
    :param args: An argparse object containing the following properties:
            db_name: A string containing a filename for the database.
    :return: Nothing.
    """
    status(args)
    confirm = raw_input("Are you sure that you want to reset the status of all jobs? Enter 'yes': ")
    if confirm.lower() == 'yes':
        conn, c = _get_or_create_db(args.db_name)
        c.execute("UPDATE jobs SET status=?, tries=?", (QUEUED, 0))
        _commit_and_close(conn, c)
        print("All jobs reset.")


def remove(args):
    _print_not_implemented()


def _set_gpu(args):
    os.environ['CUDA_VISIBLE_DEVICES'] = args.gpu
    print('Set CUDA_VISIBLE_DEVICES to', str(os.environ['CUDA_VISIBLE_DEVICES']))


if __name__ == '__main__':
    options_parser = argparse.ArgumentParser(prog="Options", add_help=False)
    options_parser.add_argument("--db_name", help="Choose a specific name for the job database. Default: joblist.sqlite3",
                                default="joblist.sqlite3")

    parser = argparse.ArgumentParser(prog="NebuLight")
    subparsers = parser.add_subparsers(title="Actions")

    sp = subparsers.add_parser("add", help="Add a single job from the command line to the queue.", parents=[options_parser])
    sp.set_defaults(func=add)
    sp.add_argument('job', help='Command to execute.')

    sp = subparsers.add_parser("add_list", help="Add a list of jobs (one per line) from a file to the queue.",
                               parents=[options_parser])
    sp.set_defaults(func=add_list)
    sp.add_argument('joblist', help='File containing commands to execute.')

    sp = subparsers.add_parser("status", help="Print the current job status.", parents=[options_parser])
    sp.set_defaults(func=status)

    sp = subparsers.add_parser("start", help="Start a worker instance locally.", parents=[options_parser])
    sp.add_argument('--max_idle_minutes', help='Maximum number of minutes to wait for new jobs before quitting.',
                    default=30, type=int)
    sp.add_argument("--gpu", help="Set CUDA_VISIBLE_DEVICES environment variable before execution.")
    sp.add_argument("--max_failures", help="Maximum number of failures for job before it is abandoned.", default=3)
    sp.set_defaults(func=start)

    sp = subparsers.add_parser("remove", help="Remove a specific job.", parents=[options_parser])
    sp.set_defaults(func=remove)

    sp = subparsers.add_parser("reset", help="Set all jobs to 'queued'.", parents=[options_parser])
    sp.set_defaults(func=reset)

    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    print('Executing stuff in',  os.path.dirname(sys.argv[0]))
    args.func(args)
