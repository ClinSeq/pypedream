import logging
import subprocess
import re
import runner

__author__ = 'dankle'

walltime = "12:00:00"  # default to 12 hours


class Slurmrunner(runner.Runner):
    def __init__(self):
        pass

    def run(self, pipeline):
        self.checkSlurmVersion()
        ordered_jobs = pipeline.get_ordered_jobs_to_run()

        for job in ordered_jobs:
            depjobids = [t.jobid for t in job.dependencies]
            if depjobids:
                depstring = "--dependency=afterok:" + ":".join(str(j) for j in depjobids)  # join job ids and stringify
            else:
                depstring = ""
            cmd = ["sbatch"]
            cmd = cmd + ["-J", job.get_name()]
            cmd = cmd + ["-t", walltime]
            cmd = cmd + ["-n", job.threads]
            cmd = cmd + ["-o", job.log]
            cmd = cmd + [depstring]
            cmd = cmd + [job.script]
            cmd = filter(None, cmd)  # removes empty elements from the list

            print "Submitting job with command: " + str(cmd)
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            msg = p.stdout.read()
            m = re.search("\d+", msg)
            jobid = m.group()
            print "Submitted job with id " + str(jobid)
            job.jobid = jobid

    def checkSlurmVersion(self):
        cmd = ["sbatch", "--version"]
        try:
            p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            msg = p.communicate()
            logging.debug("Found Slurm: {}".format(msg))
        except OSError:
            raise OSError("SLURM (sbatch) not found in system path. Quitting")

