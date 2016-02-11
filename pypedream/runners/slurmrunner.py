import runner

__author__ = 'dankle'

import sys
import subprocess
import re

jobnamePrefix = "pypedream."


class Slurmrunner(runner.Runner):
    def run(self, pipeline):
        if not pipeline.account:
            print "Slurm required parameter account (\"-A\") not set."
            sys.exit(1)
        self.checkSlurmVersion()
        ordered_tasks = pipeline.getOrderedTasks()
        for task in ordered_tasks:
            jobname = jobnamePrefix + task.taskname.encode("utf8")  # reencode to remove u prefix)
            depjobids = [t.jobid for t in task.dependencies]
            if depjobids:
                depstring = "--dependency=afterok:" + ":".join(str(j) for j in depjobids)  # join job ids and stringify
            else:
                depstring = ""
            cmd = ["sbatch"]
            cmd = cmd + ["-J", jobname]
            cmd = cmd + ["-t", task.walltime]
            cmd = cmd + ["-A", pipeline.account]
            cmd = cmd + ["-o", task.log]
            cmd = cmd + [depstring]
            cmd = cmd + [task.script]
            cmd = filter(None, cmd)  # removes empty elements from the list

            print "Submitting job with command: " + str(cmd)
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            msg = p.stdout.read()
            m = re.search("\d+", msg)
            jobid = m.group()
            print "Submitted job with id " + str(jobid)
            task.jobid = jobid

    def checkSlurmVersion(self):
        cmd = ["sbatch", "--version"]
        try:
            p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            msg = p.communicate()
            print "Found Slurm: " + str(msg)
        except OSError:
            print "SLURM (sbatch) not found in system path. Quitting"
            sys.exit(1)


# http://stackoverflow.com/questions/480214
def uniq(seq):  # renamed from f7()
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]
