import datetime
import logging
import time

from localq.localQ_server import LocalQServer
from localq.status import Status

from pypedream.pypedreamstatus import PypedreamStatus
from pypedream.runners.runner import Runner

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
        self.ordered_jobs = None

    def run(self, pipeline):
        """
        Run a pipeline using LocalQ
        :type pipeline: PypedreamPipeline
        :return:
        """

        self.pipeline = pipeline
        self.server = LocalQServer(num_cores_available=self.threads, interval=0.1)
        ordered_jobs_to_run = self.pipeline.get_ordered_jobs_to_run()
        self.ordered_jobs = self.pipeline.get_ordered_jobs()
        logging.info("Starting")
        time.sleep(2)

        for job in ordered_jobs_to_run:
            depjobs = self.pipeline.get_dependencies(job)
            depjobids = [j.jobid for j in depjobs if j.status != PypedreamStatus.COMPLETED]

            job.jobid = self.server.add_script(job.script, job.threads, stdout=job.log, stderr=job.log,
                                               name=job.get_name(), dependencies=depjobids)
            if not job.jobid:
                raise ValueError(
                    "Job {} could not be submitted with {} requested cores. {} cores available on the localq server.".format(
                        job.get_name(), job.threads, self.server.num_cores_available))

        self.pipeline.write_jobdb_json()

        n_pending = len([j for j in self.ordered_jobs if j.status == PypedreamStatus.PENDING])
        n_done = len([j for j in self.ordered_jobs if j.status == PypedreamStatus.COMPLETED])
        n_failed = len([j for j in self.ordered_jobs if j.status == PypedreamStatus.FAILED])
        n_running = len([j for j in self.ordered_jobs if j.status == PypedreamStatus.RUNNING])

        self.server.run()

        start_time = datetime.datetime.now()
        last_time = datetime.datetime.now()

        logging.info("Pipeline starting with {} jobs.".format(len(self.ordered_jobs)))
        logging.info("{} Pending/{} Running/{} Done/{} Failed".format(n_pending, n_running, n_done, n_failed))

        while not self.is_done():
            time.sleep(1)
            self.update_job_status()

            job_status = [j.status for j in self.ordered_jobs]
            n_pending = len([s for s in job_status if s == PypedreamStatus.PENDING])
            n_done_current = len([s for s in job_status if s == PypedreamStatus.COMPLETED])
            n_failed_current = len([s for s in job_status if s == PypedreamStatus.FAILED])
            n_running = len(self.ordered_jobs) - n_pending - n_done_current - n_failed_current

            if n_done_current > n_done or n_failed_current > n_failed:
                n_done = n_done_current
                n_failed = n_failed_current
                if n_failed_current > 0:
                    jf = [j for j in self.ordered_jobs if j.status == PypedreamStatus.FAILED]
                    logging.debug("Jobs {} have failed".format([j.jobid for j in jf]))
                logging.info("{} Pending/{} Running/{} Done/{} Failed".format(n_pending, n_running, n_done, n_failed))

                # self.pipeline.cleanup()

        self.pipeline.cleanup()
        d = self.get_job_status_dict()
        return_code = d[PypedreamStatus.FAILED] + d[PypedreamStatus.CANCELLED]
        self.update_job_status()

        return return_code

    def is_done(self):
        """
        Logic to tell if a server has finished.
        :param server:
        :return:
        """
        if self.server.get_runnable_jobs():
            # if there are still jobs that can run, we're not done
            return False
        elif Status.RUNNING in self.server.get_status_all().values():
            # if any jobs are running, we're not done
            return False
        else:
            # otherwise, we're done!
            return True

    def update_job_status(self):
        for localqjob in self.server.get_ordered_jobs():
            pypedreamjob = self.pipeline.get_job_with_id(localqjob.jobid)
            pypedreamjob.status = localqjob.status()

            if pypedreamjob.starttime is None:
                pypedreamjob.starttime = localqjob.start_time
            if pypedreamjob.endtime is None:
                pypedreamjob.endtime = localqjob.end_time

            if pypedreamjob.status == PypedreamStatus.COMPLETED:
                # pypedreamjob.try_remove_files(pypedreamjob.failfiles())
                # pypedreamjob.touch_files(pypedreamjob.donefiles())
                pypedreamjob.complete()
            elif pypedreamjob.status == PypedreamStatus.FAILED:
                # pypedreamjob.try_remove_files(pypedreamjob.donefiles())
                # pypedreamjob.touch_files(pypedreamjob.failfiles())
                pypedreamjob.fail()

        self.pipeline.write_jobdb_json()

    def get_job_stats(self):
        return [j.info_dict() for j in self.server.graph.nodes()]

    def stop_all_jobs(self):
        logging.error("Killing running jobs...")
        self.server.stop_all_jobs()

    def get_job_status(self, jobid):
        job = [j for j in self.ordered_jobs if j.jobid == jobid][0]
        return job.status

    def get_job_status_dict(self, fractions=False):
        """
        Get a dictionary with number of jobs for each status
        :rtype: dict[PypedreamStatus, int]
        """
        d = {}
        for st in [PypedreamStatus.COMPLETED, PypedreamStatus.FAILED, PypedreamStatus.PENDING,
                   PypedreamStatus.RUNNING, PypedreamStatus.CANCELLED, PypedreamStatus.NOT_FOUND]:
            n = len([j for j in self.ordered_jobs if self.get_job_status(j.jobid) == st])
            d[st] = n
        if fractions:
            tot = sum(d.values())
            for st in d:
                d[st] = float(d[st]) / tot
        return d


# http://stackoverflow.com/questions/480214
def uniq(seq):  # renamed from f7()
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]
