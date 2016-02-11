import logging
from click import progressbar
import runner
import subprocess

__author__ = 'dankle'


def get_job_name(job):
    if job is not None:
        return job.get_name()
    else:
        return "Done!"


class Shellrunner(runner.Runner):
    def __init__(self):
        pass

    def run(self, pipeline):
        """
        Run the submitted pipeline
        :param pipeline:
        :return: True if no errors occurred, False otherwise
        """
        ordered_jobs = pipeline.get_ordered_jobs_to_run()
        # ordered_jobs = pipeline.get_ordered_jobs()
        with progressbar(ordered_jobs, item_show_func=get_job_name) as bar:
            for job in bar:
                logging.debug("Running {} with script {}".format(job.get_name(), job.script))
                cmd = ["sh", job.script]
                logfile = open(job.log, 'w')
                logging.debug("writing to log {}".format(job.log))
                p = subprocess.Popen(cmd, stdout=logfile, stderr=logfile)
                returncode = p.wait()
                logfile.flush()
                if returncode != 0:
                    f = open(job.log)
                    logging.error("Task " + job.get_name() + " failed with exit code " + str(returncode))
                    logging.error("Contents of " + job.log + ":")
                    logging.error(f.read())
                    f.close()
                    job.fail()
                    raise OSError
                else:
                    job.complete()

                pipeline.cleanup()

                logfile.close()

        return True
