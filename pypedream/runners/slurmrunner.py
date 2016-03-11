import logging
import subprocess
import re
import runner

__author__ = 'dankle'

walltime = "12:00:00"  # default to 12 hours


class Slurmrunner(runner.Runner):
    def __init__(self):
        self.pipeline = None

    def run(self, pipeline):
        self.pipeline = pipeline
        self.checkSlurmVersion()
        ordered_jobs = pipeline.get_ordered_jobs_to_run()
        
        for job in ordered_jobs:
            depjobs = self.pipeline.get_dependencies(job)
            depjobids = [j.jobid for j in depjobs if j in ordered_jobs]
            if depjobids and depjobids is not []:
                depstring = "--dependency=afterok:" + ":".join(str(j) for j in depjobids)  # join job ids and stringify
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

            logging.info("Submitting job with command: {}".format(cmd))
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            msg = p.stdout.read()
            m = re.search("\d+", msg)
            jobid = m.group()
            logging.info("Submitted job with id " + str(jobid))
            job.jobid = jobid
        return 0

    def checkSlurmVersion(self):
        cmd = ["sbatch", "--version"]
        try:
            p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            msg = p.communicate()
            logging.debug("Found Slurm: {}".format(msg))
        except OSError:
            raise OSError("SLURM (sbatch) not found in system path. Quitting")

