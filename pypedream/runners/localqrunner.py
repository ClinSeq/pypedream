import time
from click import progressbar
from localq.localQ_server import LocalQServer
from localq.status import Status

from pypedream.pypedreamstatus import PypedreamStatus

import runner
from pypedream.pipeline.pypedreampipeline import PypedreamPipeline

__author__ = 'dankle'

import sys
import subprocess
import re

jobnamePrefix = "pypedream."


def check_completed_jobs(jobs):
    for job in jobs:
        if job.status == PypedreamStatus.COMPLETED:
            job.complete()
        elif job.status == PypedreamStatus.FAILED:
            job.fail()


class Localqrunner(runner.Runner):
    def run(self, pipeline):
        """
        Run a pipeline using LocalQ
        :type pipeline: PypedreamPipeline
        :return:
        """
        server = LocalQServer(num_cores_available=2, interval=0.1)

        ordered_jobs_to_run = pipeline.get_ordered_jobs_to_run()
        all_ordered_jobs = pipeline.get_ordered_jobs()
        for job in ordered_jobs_to_run:
            depjobs = pipeline.get_dependencies(job)
            depjobids = [j.jobid for j in depjobs if j.status != PypedreamStatus.COMPLETED]

            #sys.stderr.write("job wants {} cores\n".format(job.threads))

            job.jobid = server.add_script(job.script, job.threads, stdout=job.log, stderr=job.log,
                              name=job.get_name(), dependencies=depjobids)
            #sys.stderr.write("added job {} with deps {}\n".format(job.jobid, depjobids))
        server.run()

        def get_jobstrs(x):
            running_jobs = [j.get_name() for j in all_ordered_jobs if j.status == PypedreamStatus.RUNNING]
            return str(",".join(running_jobs))

        with progressbar(length=len(all_ordered_jobs), item_show_func=get_jobstrs) as bar:
            n_done = len([j for j in all_ordered_jobs if j.status == PypedreamStatus.COMPLETED])
            bar.update(n_done)
            while True:
                time.sleep(0.1)
                n_done_current = len([j for j in all_ordered_jobs if j.status == PypedreamStatus.COMPLETED])
                n_done_new = n_done_current - n_done
                bar.update(n_done_new)
                n_done = n_done_current
                # for all jobs in localq, get corresponding pipeline jobs
                # update the pipeline job's status
                for localqjob in server.get_ordered_jobs():
                    pypedreamjob = pipeline.get_job_with_id(localqjob.jobid)
                    pypedreamjob.status = localqjob.status()

                check_completed_jobs(ordered_jobs_to_run)

                if n_done == len(all_ordered_jobs):
                    break

            pipeline.cleanup()


# http://stackoverflow.com/questions/480214
def uniq(seq):  # renamed from f7()
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]
