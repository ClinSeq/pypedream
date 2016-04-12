import logging
import re
import subprocess
import sys
import time

import runner
from pypedream.job import Job
from pypedream.pypedreamstatus import PypedreamStatus

__author__ = 'dankle'

walltime = "12:00:00"  # default to 12 hours


class Slurmrunner(runner.Runner):
    def __init__(self, interval=30):
        """
        Run jobs on a slurm cluster.
        """
        self.pipeline = None
        self.ordered_jobs = None
        self.interval = interval

    def run(self, pipeline):
        self.pipeline = pipeline
        self.checkSlurmVersion()
        self.ordered_jobs = pipeline.get_ordered_jobs_to_run()

        #try:
        for job in self.ordered_jobs:
            depjobs = self.pipeline.get_dependencies(job)
            depjobids = [j.jobid for j in depjobs if j in self.ordered_jobs]
            if depjobids and depjobids is not []:
                depstring = "--dependency=afterok:" + ":".join(
                    str(j) for j in depjobids)  # join job ids and stringify
            else:
                depstring = ""
            cmd = ["sbatch"]
            cmd = cmd + ["-J", job.get_name()]
            cmd = cmd + ["-t", walltime]
            cmd = cmd + ["-n", str(job.threads)]
            cmd = cmd + ["-o", job.log]
            cmd = cmd + [depstring]
            cmd = cmd + [job.script]
            cmd = filter(None, cmd)  # removes empty elements from the list

            logging.debug("Submitting job with command: {}".format(cmd))
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            msg = p.stdout.read()
            m = re.search("\d+", msg)
            jobid = m.group()
            logging.info("Submitted job {} with id {} ".format(job.get_name(), jobid))
            job.jobid = jobid

        while not self.is_done():
            time.sleep(self.interval)

            for job in self.ordered_jobs:
                if job.status != PypedreamStatus.COMPLETED and job.status != PypedreamStatus.FAILED:
                    newjobobj = Job.query.filter_by(jobid=jobid).first()
                    newjobobj.status = self.get_job_status(job.jobid)
            if self.pipeline.session:
                self.pipeline.session.commit()

            self.pipeline.cleanup()

        # except Exception:
        #     self.stop_all_jobs()
        #     return 1

        self.pipeline.cleanup()

        d = self.pipeline.get_job_status_dict()
        print >>sys.stderr, "jobs status is {}".format(d)
        return_code = d[PypedreamStatus.FAILED] + d[PypedreamStatus.CANCELLED]
        return return_code

    def stop_all_jobs(self):
        logging.error("Autoseq failed, cancelling jobs...")
        for job in self.ordered_jobs:
            job.fail()
            subprocess.check_output(['scancel', str(job.jobid)])
        self.pipeline.status = PypedreamStatus.FAILED

    def get_job_status(self, jobid):
        """
        Get the status of a job.

        Notes
        * Jobs might not be in accounting when they are pending
        * Jobs might be PENDING in accounting while they are RUNNING in the queue

        therefore, first check the queue. If the job is there, use its status.
        If it's not, check in accounting
        """

        status_str = Slurmrunner._get_job_status_from_squeue(jobid)
        if not status_str:
            status_str = Slurmrunner._get_job_status_from_sacct(jobid)

        # convert string to PypedreamStatus
        status = PypedreamStatus.from_slurm(status_str)

        # if we still don't have a status, use NOT_FOUND
        if not status:
            return PypedreamStatus.NOT_FOUND

        return status

    @staticmethod
    def _get_job_status_from_squeue(jobid):
        """
        Try to get job status string from the queue. If it's not found, return None
        """
        # '(null)|(null)|1|0|2016-04-11T07:50:01|(null)|vagrant|unknwn|43|sleep.sh|(null)|UNLIMITED|0||/vagrant/sleep.sh|0.99998473585583|(null)|None||CD|vagrant|(null)|(null)||0|*:*:*|43 |fairbanksdev |1 |1 | |43 |1000 |* |* |* |N/A |UNLIMITED |0:05 |fairbanksdev |0 |core |4294901736 |fairbanksdev |2016-04-11T07:49:56 |COMPLETED |1000 |2016-04-11T07:49:56 |(null) |N/A |(null) |/home/vagrant \n'
        # squeue -j 42 --noheader -t all -o %all
        status = None

        short2long = {"PD": "PENDING",
                      "R": "RUNNING",
                      "S": "SUSPENDED",
                      "ST": "STOPPED",
                      "CG": "COMPLETING",
                      "CD": "COMPLETED",
                      "CF": "CONFIGURING",
                      "CA": "CANCELLED",
                      "F": "FAILED",
                      "TO": "TIMEOUT",
                      "PR": "PREEMPTED",
                      "BF": "BOOT_FAIL",
                      "NF": "NODE_FAIL",
                      "SE": "SPECIAL_EXIT"
                      }

        try:
            cmd = ['squeue', '-j', str(jobid), '--noheader', '-t', 'all', '-o', '%all']
            stdout = subprocess.check_output(cmd)
            short_status = stdout.strip().split("|")[19]  # 20th element is shorthand version of the status
            if short_status in short2long:
                status = short2long[short_status]

        except subprocess.CalledProcessError:
            pass

        return status

    @staticmethod
    def _get_job_status_from_sacct(jobid):
        """
        Get job status string from accounting
        """
        cmd = ['sacct', '-j', str(jobid), '-b', '-P', 'noheader']
        stdout = subprocess.check_output(cmd)
        status = None
        if stdout != '':
            jobid_ret, status, exitcode = stdout.strip().split("|")

        return status

    def checkSlurmVersion(self):
        cmd = ["sbatch", "--version"]
        try:
            p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            msg = p.communicate()
            logging.debug("Found Slurm: {}".format(msg))
        except OSError:
            raise OSError("SLURM (sbatch) not found in system path. Quitting")

    def get_runnable_jobs(self):
        """
        Get a list of pending jobs that are ready to be run based on dependencies
        :return: List of Jobs
        """
        pending_jobs = [j for j in self.ordered_jobs if j.status == PypedreamStatus.PENDING]
        ready_jobs = []
        for job in pending_jobs:
            depjobs = self.pipeline.get_dependencies(job)
            depjobids = [j.jobid for j in depjobs if j in self.ordered_jobs]

            # if there are no dependencies, the job is always ready
            if not depjobids:
                ready_jobs.append(job)
            else:
                # get a list containing the status of all dependencies
                dependency_status = [self.get_job_status(depid) for depid in depjobids]
                # if a uniqiefied list contains a single element, and that element is "COMPLETED"
                # then the job is ready
                if len(set(dependency_status)) == 1 and dependency_status[0] == PypedreamStatus.COMPLETED:
                    ready_jobs.append(job)
        return ready_jobs

    def is_done(self):
        """
        Logic to tell if a server has finished.
        :param server:
        :return:
        """
        if self.get_runnable_jobs():
            # if there are still jobs that can run, we're not done
            return False
        elif PypedreamStatus.RUNNING in [j.status for j in self.ordered_jobs]:
            # if any jobs are running, we're not done
            return False
        else:
            # otherwise, we're done!
            return True
