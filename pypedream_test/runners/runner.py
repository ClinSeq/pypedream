import abc
import json

__author__ = 'dankle'


class Runner(object):
    """ An abstract class that can run scripts on the cli or submit to a queue.
        Concrete implementations of Runner have to implement the run() method which runs the jobs.
        The pipeline is responsible for generating a list of ordered jobs to run.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self):
        pass

    @abc.abstractmethod
    def run(self, pipeline):
        pass

    @abc.abstractmethod
    def get_job_status(self, jobid):
        pass

