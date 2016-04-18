import inspect
import json
import logging
import multiprocessing
import os
import sys
from multiprocessing import Process

import datetime
import networkx as nx
from networkx.drawing.nx_pydot import write_dot
from pypedream.runners import slurmrunner

import pypedream.constants
from pypedream.job import Job
from pypedream.pypedreamstatus import PypedreamStatus
from pypedream.runners.shellrunner import Shellrunner

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

__author__ = 'dankle'


class PypedreamPipeline(Process):
    runner = None
    starttime = None
    endtime = None
    status = None

    def __init__(self, outdir, scriptdir=None, dot_file=None, runner=Shellrunner(), jobdb=None):
        Process.__init__(self)
        self.status = PypedreamStatus.PENDING
        self.graph = nx.MultiDiGraph()
        self.dot_file = dot_file
        self.runner = runner
        self.outdir = outdir
        self.jobdb = jobdb
        self.exit = multiprocessing.Event()

        if not scriptdir:
            self.scriptdir = "{}/.pypedream/scripts/".format(self.outdir)

        logging.debug("Initialized PypedreamPipeline with parameters: {}".format({'outdir': self.outdir,
                                                                                  'scriptdir': self.scriptdir,
                                                                                  'runner': self.runner.__class__,
                                                                                  'dot_file': self.dot_file}))

    def add(self, job):
        """
        :type job:Job
        """
        logging.debug("Added step {}".format(job.get_name()))
        inputs = []
        outputs = []
        for varname in job.__dict__:
            if varname.startswith(pypedream.constants.INPUT):
                inputs.append(varname)

        for varname in job.__dict__:
            if varname.startswith(pypedream.constants.OUTPUT):
                outputs.append(varname)

        logging.debug("  inputs: " + str(inputs))
        logging.debug("  outputs: " + str(outputs))
        job.set_log()
        logging.debug("Will write log to {}".format(job.log))
        logging.debug("inputs are: {}".format(job.get_inputs()))
        logging.debug("outputs are: {}".format(job.get_outputs()))
        logging.debug("donefiles are: {}".format(job.donefiles()))
        logging.debug("failfiles are: {}".format(job.failfiles()))

        if job.all_donefiles_exists():
            job.status = PypedreamStatus.COMPLETED

        self.graph.add_node(job)
        job.name = job.get_name()

    def add_edges(self):
        filenames = self.get_all_files()
        for fname in filenames:
            inputs = self.get_nodes_with_input(fname)
            outputs = self.get_nodes_with_output(fname)
            if inputs and outputs:
                for i in inputs:
                    for o in outputs:
                        if i not in self.graph.nodes():
                            raise ValueError("input node {} is not in graph".format(i))
                        if o not in self.graph.nodes():
                            raise ValueError("output node {} is not in graph".format(o))
                        logging.debug(
                            "Adding edge from " + o.get_name() + " to " + i.get_name() + " with name " + fname)

                        self.graph.add_edges_from([(o, i)], filename=fname)

        self.write_scripts()

    def get_all_files(self):
        """
        Get names of all files added as inputs or outputs
        :return:
        """
        files = []
        for tool in self.graph.nodes():
            for varname in tool.__dict__:
                if varname.startswith(pypedream.constants.INPUT) or \
                        varname.startswith(pypedream.constants.OUTPUT):  # input and outputs are files
                    obj = tool.__dict__[varname]  # can be a list or a string
                    if obj.__class__.__name__ == "str":
                        files.append(obj)
                    elif obj.__class__.__name__ == "list":
                        for item in obj:
                            files.append(item)
        return list(set(filter(None, files)))  # filter removes empties, list(set()) uniquifies the list.

    def get_nodes_with_input(self, filename):
        """ Get list of nodes (tools) that has "filename" as an input
        :param filename: name of file to search for
        :return: list of tools
        """
        tools = []
        for tool in self.graph.nodes():
            for varname in tool.__dict__:
                if varname.startswith(pypedream.constants.INPUT):
                    obj = tool.__dict__[varname]  # can be a list or a string
                    if obj.__class__.__name__ == "str" and obj == filename:
                        tools.append(tool)
                    elif obj.__class__.__name__ == "list" and filename in obj:
                        tools.append(tool)
        return tools

    def get_dependencies(self, job):
        """
        Get dependencies
        :type job: job.Job
        :rtype: list[job.job]
        """
        depjobs = []
        for inf in job.get_inputs():
            depjobs += self.get_nodes_with_output(inf)
        return uniq(depjobs)

    def get_job_with_id(self, jobid):
        if jobid is None:
            return None
        for job in self.graph.nodes():
            if 'jobid' in job.__dict__:
                if job.jobid == jobid:
                    return job
        return None

    def get_outputs(self):
        outputs = []
        for job in self.graph.nodes():
            outputs += job.get_outputs()
        return outputs

    def get_nodes_with_output(self, filename):
        """ Get list of nodes (tools) that has "filename" as an output
        :param filename: name of file to search for
        :return: list of tools
        """
        tools = []
        for tool in self.graph.nodes():
            for varname in tool.__dict__:
                if varname.startswith(pypedream.constants.OUTPUT):
                    obj = tool.__dict__[varname]  # can be a list or a string
                    if obj == filename:
                        tools.append(tool)
        return tools

    def write_dot(self):
        """ Write a dot file with the pipeline graph
        :param f: file to write
        :return: None
        """
        if self.dot_file:
            write_dot(self.graph, self.dot_file)

    def write_scripts(self):
        """
        Write scripts for all steps in pipeline
        :param scriptdir: dir to write scripts
        :return:
        """
        logging.debug("-----------------------------------------------------")
        logging.debug("Writing scripts.")
        if not os.path.exists(self.scriptdir):
            logging.debug("Output directory " + self.scriptdir + " does not exist. Creating. ")
            os.makedirs(self.scriptdir)

        for job in self.get_ordered_jobs():
            job.write_script(self.scriptdir, self)

        logging.debug("-----------------------------------------------------")

    def get_ordered_jobs(self):
        """ Method to order the tasks in the pipeline
        :return: An array of paths for the runner to run
        """
        if not nx.is_directed_acyclic_graph(self.graph):
            raise ValueError("ERROR: The submitted pipeline is not a DAG. Check the pipeline for loops.")

        ordered_jobs = nx.topological_sort(self.graph)
        return ordered_jobs

    def get_ordered_jobs_to_run(self):
        all_jobs = self.get_ordered_jobs()
        jobs_to_run = []
        for job in all_jobs:
            if job.status != PypedreamStatus.COMPLETED:
                jobs_to_run.append(job)

        return jobs_to_run

    def cleanup(self):
        for output_file in self.get_outputs():
            keep_file = False

            # if any job that has this file as an input is not yet done, keep the file
            jobs = self.get_nodes_with_input(output_file)
            statuses = set([j.status for j in jobs])
            if statuses != set([PypedreamStatus.COMPLETED]):
                keep_file = True

            # if the job that generated this file is marked as !is_intermediate, keep the file
            for job in self.get_nodes_with_output(output_file):
                if not job.is_intermediate:
                    keep_file = True

            if not keep_file and os.path.exists(output_file):
                logging.debug("removing intermediate file {}".format(output_file))
                os.remove(output_file)

    def total_jobs(self):
        return sum(self.get_job_status_dict().values())

    def run(self):
        self.starttime = datetime.datetime.now().isoformat()
        self.status = PypedreamStatus.RUNNING
        self.add_edges()

        if self.dot_file:
            self.write_dot()

        self.write_jobs()
        return_code = self.runner.run(self)

        self.endtime = datetime.datetime.now().isoformat()

        if return_code == 0:
            logging.info("Pipeline finished successfully. ")
            self.status = PypedreamStatus.COMPLETED
        else:
            if return_code == slurmrunner.exitcode_cancelled:
                self.status = PypedreamStatus.CANCELLED
            elif return_code == slurmrunner.exitcode_failed:
                self.status = PypedreamStatus.FAILED
            else:
                self.status = PypedreamStatus.FAILED
            logging.info("Pipeline failed with exit code {}.".format(return_code))

        self.write_jobs()
        sys.exit(return_code)

    def stop(self):
        self.exit.set()

    def stop_all_jobs(self):
        self.runner.stop_all_jobs()

    def write_jobs(self):
        if self.jobdb:
            with open(self.jobdb, 'w') as f:
                jobs = []
                for j in self.get_ordered_jobs():
                    jobs.append({'jobname': j.jobname,
                                 'status': j.status,
                                 'jobid': j.jobid,
                                 'inputs': j.get_inputs(),
                                 'outputs': j.get_outputs(),
                                 'starttime': j.starttime,
                                 'endtime': j.endtime,
                                 'threads': j.threads,
                                 'log': j.log
                                 })

                d = {'jobs': jobs,
                     'starttime': self.starttime,
                     'endtime': self.endtime,
                     'exitcode': self.exitcode
                     }

                json.dump(d, f, indent=4)


# http://stackoverflow.com/questions/480214
def uniq(seq):  # renamed from f7()
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]
