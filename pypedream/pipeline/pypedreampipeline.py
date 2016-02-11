import inspect
import logging
import os
import sys
import networkx as nx
import pypedream.constants
from pypedream.job import Job
from pypedream.pypedreamstatus import PypedreamStatus

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

__author__ = 'dankle'

default_scriptdir = "/tmp/.pypedream"


class PypedreamPipeline:
    def __init__(self, scriptdir=default_scriptdir, run=None, dot=None):
        self.graph = nx.MultiDiGraph()
        self.scriptdir = os.path.abspath(os.path.expandvars(os.path.expanduser(scriptdir)))
        self.run = run
        self.dot = dot
        logging.debug("instantiated a pipeline with scriptdir {}".format(self.scriptdir))

    def add(self, job):
        """
        :type job:Job
        """
        logging.info("Added step {}".format(job.get_name()))
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
        logging.info("Will write log to {}".format(job.log))
        if job.all_donefiles_exists():
            job.status = PypedreamStatus.COMPLETED

        self.graph.add_node(job)

    def add_edges(self):
        filenames = self.get_all_files()
        for fname in filenames:
            inputs = self.get_nodes_with_input(fname)
            outputs = self.get_nodes_with_output(fname)
            if inputs and outputs:
                for i in inputs:
                    for o in outputs:
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

    def write_dot(self, f):
        """ Write a dot file with the pipeline graph
        :param f: file to write
        :return: None
        """
        nx.write_dot(self.graph, f)

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
            print "ERROR: The submitted pipeline is not a DAG. Check the pipeline for loops."
            raise ValueError

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
            delete_file = True

            # if any job that has this file as an input is not yet done, keep the file
            for job in self.get_nodes_with_input(output_file):
                if job.status != PypedreamStatus.COMPLETED:
                    delete_file = False

            # if the job that generated this file is marked as !is_intermediate, keep the file
            for job in self.get_nodes_with_output(output_file):
                if not job.is_intermediate:
                    delete_file = False

            if delete_file and os.path.exists(output_file):
                os.remove(output_file)

        for job in self.graph.nodes():
            input_files = job.get_inputs()


# http://stackoverflow.com/questions/480214
def uniq(seq):  # renamed from f7()
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]
