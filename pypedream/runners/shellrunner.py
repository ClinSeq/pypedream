import logging
import subprocess

import sys

import datetime
from click import progressbar

import runner
from pypedream.pypedreamstatus import PypedreamStatus

__author__ = 'dankle'


def get_job_name(job):
    if job is not None:
        return job.get_name()
    else:
        return "Done!"


class Shellrunner(runner.Runner):
    def __init__(self):
        self.pipeline = None

    def run(self, pipeline):
        """
        Run the submitted pipeline
        :param pipeline:
        :return: True if no errors occurred, False otherwise
        """
        self.pipeline = pipeline
        ordered_jobs = self.pipeline.get_ordered_jobs_to_run()

        # ordered_jobs = pipeline.get_ordered_jobs()
        with progressbar(ordered_jobs, item_show_func=get_job_name) as bar:
            for job in bar:
                logging.debug("Running {} with script {}".format(job.get_name(), job.script))
                cmd = ["bash", job.script]
                logfile = open(job.log, 'w')
                logging.debug("writing to log {}".format(job.log))
                job.status = PypedreamStatus.RUNNING
                job.starttime = datetime.datetime.now().isoformat()
                proc = None
                try:
                    proc = subprocess.check_call(cmd, stdout=logfile, stderr=logfile)
                    job.endtime = datetime.datetime.now().isoformat()
                    job.complete()
                except subprocess.CalledProcessError as err:
                    job.endtime = datetime.datetime.now().isoformat()
                    job.fail()
                    self.pipeline.write_jobdb_json()
                    logfile.flush()
                    with open(job.log, 'r') as logf:
                        logging.warning("Task {} failed with exit code {}".format(job.get_name(),
                                                                                  err.returncode))
                        logging.warning("Contents of " + job.log + ":")
                        logging.warning(logf.read())
                    return err.returncode

                self.pipeline.cleanup()
                self.pipeline.write_jobdb_json()
                logfile.close()

        return 0

    def get_job_status(self, jobid):
        return self.pipeline.get_job_with_id(jobid).status
