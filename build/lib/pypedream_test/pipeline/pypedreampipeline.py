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

logger = logging.getLogger(__name__)

__author__ = 'dankle'


class PypedreamPipeline(Process):
    runner = None
    starttime = None
    endtime = None
    status = None
    runner_returncode = None

    def __init__(self, outdir, scriptdir=None, dot_file=None, runner=Shellrunner(), jobdb=None, scratch="/tmp"):
        Process.__init__(self)
        self.status = PypedreamStatus.PENDING
        self.graph = nx.MultiDiGraph()
        self.dot_file = dot_file
        self.runner = runner
        self.outdir = outdir
        self.jobdb = jobdb
        self.scratch = scratch
        self.exit = multiprocessing.Event()

        if not scriptdir:
            self.scriptdir = "{}/.pypedream/scripts/".format(self.outdir)

        logger.debug("Initialized PypedreamPipeline with parameters: {}".format({'outdir': self.outdir,
                                                                                 'scriptdir': self.scriptdir,
                                                                                 'runner': self.runner.__class__,
                                                                                 'dot_file': self.dot_file}))

    def add(self, job):
        """
        :type job:Job
        """
        logger.debug("Added step {}".format(job.get_name()))
        inputs = []
        outputs = []
        for varname in job.__dict__:
            if varname.startswith(pypedream.constants.INPUT):
                inputs.append(varname)

        for varname in job.__dict__:
            if varname.startswith(pypedream.constants.OUTPUT):
                outputs.append(varname)

        logger.debug("  inputs: " + str(inputs))
        logger.debug("  outputs: " + str(outputs))
        job.set_log()
        logger.debug("Will write log to {}".format(job.log))
        logger.debug("inputs are: {}".format(job.get_inputs()))
        logger.debug("outputs are: {}".format(job.get_outputs()))
        logger.debug("donefiles are: {}".format(job.donefiles()))
        logger.debug("failfiles are: {}".format(job.failfiles()))

        if job.all_donefiles_exists():
            job.status = PypedreamStatus.COMPLETED

        self.graph.add_node(job)
        job.name = job.get_name()

    def _add_edges(self):
        filenames = self._get_all_files()
        for fname in filenames:
            inputs = self._get_nodes_with_input(fname)
            outputs = self._get_nodes_with_output(fname)
            if inputs and outputs:
                for i in inputs:
                    for o in outputs:
                        if i not in self.graph.nodes():
                            raise ValueError("input node {} is not in graph".format(i))
                        if o not in self.graph.nodes():
                            raise ValueError("output node {} is not in graph".format(o))
                        logger.debug(
                            "Adding edge from " + o.get_name() + " to " + i.get_name() + " with name " + fname)

                        self.graph.add_edges_from([(o, i)], filename=fname)

        self._write_scripts()

    def _get_all_files(self):
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

    def _get_nodes_with_input(self, filename):
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

    def _get_dependencies(self, job):
        """
        Get dependencies
        :type job: job.Job
        :rtype: list[job.job]
        """
        depjobs = []
        for inf in job.get_inputs():
            depjobs += self._get_nodes_with_output(inf)
        return uniq(depjobs)

    def _get_job_with_id(self, jobid):
        if jobid is None:
            return None
        for job in self.graph.nodes():
            if 'jobid' in job.__dict__:
                if job.jobid == jobid:
                    return job
        return None

    def _get_outputs(self):
        outputs = []
        for job in self.graph.nodes():
            outputs += job.get_outputs()
        return outputs

    def _get_nodes_with_output(self, filename):
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

    def _write_dot(self):
        """ Write a dot file with the pipeline graph
        :param f: file to write
        :return: None
        """
        if self.dot_file:
            write_dot(self.graph, self.dot_file)

    def _write_scripts(self):
        """
        Write scripts for all steps in pipeline
        :param scriptdir: dir to write scripts
        :return:
        """
        logger.debug("Writing scripts.")
        if not os.path.exists(self.scriptdir):
            logger.debug("Output directory " + self.scriptdir + " does not exist. Creating. ")
            os.makedirs(self.scriptdir)

        for job in self._get_ordered_jobs():
            job.write_script(self.scriptdir, self)

    def _get_ordered_jobs(self):
        """ Method to order the tasks in the pipeline
        :return: An array of paths for the runner to run
        """
        if not nx.is_directed_acyclic_graph(self.graph):
            raise ValueError("ERROR: The submitted pipeline is not a DAG. Check the pipeline for loops.")
        ordered_jobs = nx.topological_sort(self.graph) 
        return ordered_jobs

    def _get_ordered_jobs_to_run(self):
        all_jobs = self._get_ordered_jobs()
        jobs_to_run = []
        for job in all_jobs:
            if job.status != PypedreamStatus.COMPLETED:
                jobs_to_run.append(job)

        return jobs_to_run

    def _cleanup(self):
        for output_file in self._get_outputs():
            keep_file = False

            # if any job that has this file as an input is not yet done, keep the file
            jobs = self._get_nodes_with_input(output_file)
            statuses = set([j.status for j in jobs])
            if statuses != set([PypedreamStatus.COMPLETED]):
                keep_file = True

            # if the job that generated this file is marked as !is_intermediate, keep the file
            for job in self._get_nodes_with_output(output_file):
                if not job.is_intermediate:
                    keep_file = True

            if not keep_file and os.path.exists(output_file):
                logger.debug("Removing intermediate file {}".format(output_file))
                os.remove(output_file)

    def _set_scratch(self, global_scratch, override=False):
        """
        Set the scratch dir of every added job, while not overriding any manually set scratch dir (default).
        If 'override' is set, it will ignore a previously set scratch dir.
        """
        for job in self.graph.nodes():
            if not job.scratch or override:
                job.scratch = global_scratch

    def run(self):
        self._set_scratch(self.scratch)
        self.starttime = datetime.datetime.now().isoformat()
        self.status = PypedreamStatus.RUNNING
        self._add_edges()

        if self.dot_file:
            self._write_dot()

        self.runner_returncode = self.runner.run(self)
        self.endtime = datetime.datetime.now().isoformat()

        if self.runner_returncode == 0:
            logger.info("Pipeline finished successfully. ")
            self.status = PypedreamStatus.COMPLETED
        else:
            if self.runner_returncode == slurmrunner.exitcode_cancelled:
                self.status = PypedreamStatus.CANCELLED
            elif self.runner_returncode == slurmrunner.exitcode_failed:
                self.status = PypedreamStatus.FAILED
            else:
                self.status = PypedreamStatus.FAILED
                logger.info("Pipeline failed with exit code {}.".format(self.runner_returncode))

        sys.exit(self.runner_returncode)

    def stop(self):
        self.exit.set()

    def _stop_all_jobs(self):
        self.runner.stop_all_jobs()

    def _write_jobdb_json(self, myjobs):
        if self.jobdb:
            with open(self.jobdb, 'w') as f:
                jobs = []
                for j in myjobs:
                    inputs = {}
                    outputs = {}

                    for varname in j.__dict__:
                        obj = j.__dict__[varname]  # can be a list or a string
                        if varname.startswith(pypedream.constants.INPUT):
                            inputs[varname] = obj

                        if varname.startswith(pypedream.constants.OUTPUT):
                            outputs[varname] = obj

                    jobs.append({'jobname': j.jobname,
                                 'status': j.status,
                                 'jobid': j.jobid,
                                 'inputs': inputs,
                                 'outputs': outputs,
                                 'starttime': j.starttime,
                                 'endtime': j.endtime,
                                 'threads': j.threads,
                                 'log': j.log
                                 })

                d = {'jobs': jobs,
                     'starttime': self.starttime,
                     'endtime': self.endtime,
                     'exitcode': self.runner_returncode,
                     'status': self.status
                     }

                json.dump(d, f, indent=4)


# http://stackoverflow.com/questions/480214
def uniq(seq):  # renamed from f7()
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]
