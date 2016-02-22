import logging
import time
import datetime

import sys
from click import progressbar
from localq.localQ_server import LocalQServer
from localq.status import Status
from pypedream.runners.runner import Runner
from pypedream.pypedreamstatus import PypedreamStatus
from pypedream.pipeline.pypedreampipeline import PypedreamPipeline

__author__ = 'dankle'

jobnamePrefix = "pypedream."


def check_completed_jobs(jobs):
    for job in jobs:
        if job.status == PypedreamStatus.COMPLETED:
            job.complete()
        elif job.status == PypedreamStatus.FAILED:
            job.fail()


class Localqrunner(Runner):
    def __init__(self, threads=1):
        self.pipeline = None
        self.threads = threads
        self.server = None

    def run(self, pipeline):
        """
        Run a pipeline using LocalQ
        :type pipeline: PypedreamPipeline
        :return:
        """
        self.pipeline = pipeline
        self.server = LocalQServer(num_cores_available=self.threads, interval=0.1)
        ordered_jobs_to_run = self.pipeline.get_ordered_jobs_to_run()
        all_ordered_jobs = self.pipeline.get_ordered_jobs()
        for job in ordered_jobs_to_run:
            depjobs = self.pipeline.get_dependencies(job)
            depjobids = [j.jobid for j in depjobs if j.status != PypedreamStatus.COMPLETED]
            job.try_remove_failfile()
            # sys.stderr.write("job wants {} cores\n".format(job.threads))

            job.jobid = self.server.add_script(job.script, job.threads, stdout=job.log, stderr=job.log,
                                               name=job.get_name(), dependencies=depjobids)
            # sys.stderr.write("added job {} with deps {}\n".format(job.jobid, depjobids))

        n_pending = len([j for j in all_ordered_jobs if j.status == PypedreamStatus.PENDING])
        n_done = len([j for j in all_ordered_jobs if j.status == PypedreamStatus.COMPLETED])
        n_failed = len([j for j in all_ordered_jobs if j.status == PypedreamStatus.FAILED])
        n_running = len([j for j in self.server.graph.nodes() if j.status() == Status.RUNNING])

        self.server.run()

        start_time = datetime.datetime.now()
        last_time = datetime.datetime.now()

        logging.info("Pipeline starting with {} jobs.".format(len(all_ordered_jobs)))
        logging.info("{} Pending/{} Running/{} Done/{} Failed".format(n_pending, n_running, n_done, n_failed))

        while self.server.get_runnable_jobs():
            time.sleep(1)
            self.update_job_status()

            job_status = [j.status for j in all_ordered_jobs]
            n_pending = len([s for s in job_status if s == PypedreamStatus.PENDING])
            n_done_current = len([s for s in job_status if s == PypedreamStatus.COMPLETED])
            n_failed_current = len([s for s in job_status if s == PypedreamStatus.FAILED])
            n_running = len(all_ordered_jobs) - n_pending - n_done_current - n_failed_current

            if n_done_current > n_done or n_failed_current > n_failed:
                n_done = n_done_current
                n_failed = n_failed_current
                if n_failed_current > 0:
                    jf = [j for j in all_ordered_jobs if j.status == PypedreamStatus.FAILED]
                    print [j.jobid for j in jf]
                logging.info("{} Pending/{} Running/{} Done/{} Failed".format(n_pending, n_running, n_done, n_failed))

        self.pipeline.cleanup()
        if n_failed > 0:
            raise OSError

    def update_job_status(self):
        for localqjob in self.server.get_ordered_jobs():
            pypedreamjob = self.pipeline.get_job_with_id(localqjob.jobid)
            pypedreamjob.status = localqjob.status()
            if pypedreamjob.status == PypedreamStatus.COMPLETED:
                pypedreamjob.complete()
            elif pypedreamjob.status == PypedreamStatus.FAILED:
                pypedreamjob.fail()

    def get_job_stats(self):
        return [j.info_dict() for j in self.server.graph.nodes()]


# http://stackoverflow.com/questions/480214
def uniq(seq):  # renamed from f7()
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]
