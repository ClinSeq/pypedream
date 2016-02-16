__author__ = 'dankle'

""" An abstract class that can run scripts on the cli or submit to a queue.

    Concrete implementations of Runner have to implement the run() method which runs the jobs.

    The pipeline is responsible for generating a list of ordered jobs to run.
"""


class Runner:
    def run(self, pipeline):
        raise NotImplementedError("Class %s doesn't implement run()" % (self.__class__.__name__))

    def get_job_stats(self):
        return None
