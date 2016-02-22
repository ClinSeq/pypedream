import logging
import os

import uuid

from pypedream import constants
from pypedream.pypedreamstatus import PypedreamStatus

__author__ = 'dankle'


class Job:
    """ A abstract class of a tool
    """

    def __init__(self):
        self.script = None
        self.log = None
        self.threads = 1  # default nbr of threads
        self.is_intermediate = False
        self.status = PypedreamStatus.PENDING
        self.jobid = None
        self.data = {}  # any additional data that the runner needs a job to keep track of

    def command(self):
        raise NotImplementedError("Class %s doesn't implement run()" % self.__class__.__name__)

    def get_name(self):
        if "jobname" in self.__dict__:
            return self.__dict__["jobname"]
        else:
            return "clifunc" + str(abs(hash(self)))

    def set_log(self):
        outputs = self.get_outputs()
        self.log = outputs[0] + ".out"
        mkdir(os.path.dirname(self.log))
        logging.debug("Setting logfile to {}".format(self.log))

    def get_inputs(self):
        """
        get a list of all input files for this job
        :return: list[str]
        """
        inputs = []
        for varname in self.__dict__:
            if varname.startswith(constants.INPUT):
                obj = self.__dict__[varname]  # can be a list or a string
                if obj.__class__.__name__ == "str":
                    inputs.append(obj)
                elif obj.__class__.__name__ == "list":
                    inputs += obj

        return inputs

    def get_outputs(self):
        """
        get a list of all output files for this job
        :return: list[str]
        """
        outputs = []
        for varname in self.__dict__:
            if varname.startswith(constants.OUTPUT):
                fname = self.__dict__[varname]  # type: str
                outputs.append(fname)

        return outputs

    def __hash__(self):
        """
        Custom hash function. Use all items in class except for log and script.
        :return: hash if object
        """
        items_to_exclude = ["script", "log", 'is_intermediate', 'status']
        items = self.__dict__.items()
        items_to_hash = dict()
        for item in items:
            if not item[0] in items_to_exclude:
                items_to_hash[item[0]] = item[1]
        return hash(str(items_to_hash))

    def __str__(self):
        return self.get_name

    def try_remove_failfile(self):
        for varname in self.__dict__:
            if varname.startswith(constants.OUTPUT):
                o = self.__dict__[varname]  # a output_nn cannot be a list
                odir = os.path.dirname(o)
                obase = os.path.basename(o)
                if os.path.exists(odir + "/." + obase + ".fail"):
                    os.remove(odir + "/." + obase + ".fail")

    def complete(self):
        self.status = PypedreamStatus.COMPLETED
        self.try_remove_failfile()
        for varname in self.__dict__:
            if varname.startswith(constants.OUTPUT):
                o = self.__dict__[varname]  # a output_nn cannot be a list
                odir = os.path.dirname(o)
                obase = os.path.basename(o)
                touch(odir + "/." + obase + ".done")

    def fail(self):
        self.status = PypedreamStatus.FAILED
        for varname in self.__dict__:
            if varname.startswith(constants.OUTPUT):
                o = self.__dict__[varname]  # a output_nn cannot be a list
                odir = os.path.dirname(o)
                obase = os.path.basename(o)
                touch(odir + "/." + obase + ".fail")
                if os.path.exists(odir + "/." + obase + ".done"):
                    os.remove(odir + "/." + obase + ".done")


    def all_donefiles_exists(self):
        donefiles_exist = True
        for varname in self.__dict__:
            if varname.startswith(constants.OUTPUT):
                o = self.__dict__[varname]  # a output_nn cannot be a list
                odir = os.path.dirname(o)
                obase = os.path.basename(o)
                donefile = odir + "/." + obase + ".done"
                if not os.path.exists(donefile):
                    donefiles_exist = False

        return donefiles_exist

    def write_script(self, script_dir, pipeline):
        logging.debug("Writing script for task " + self.get_name())
        hashes = [hash(job) for job in pipeline.get_ordered_jobs()]
        idx = hashes.index(hash(self))

        logging.debug("Task index is " + str(idx))
        self.script = "{dir}/{idx}-{name}-{uuid}.sh".format(dir=script_dir,
                                                            idx=idx,
                                                            name=self.get_name(),
                                                            uuid=uuid.uuid4())

        f = open(self.script, 'w')
        f.write("#!/usr/bin/env bash\n")
        f.write("set -euo pipefail\n")
        f.write("\n")

        # create directories for output files
        for varname in self.__dict__:
            if varname.startswith(constants.OUTPUT):
                fname = self.__dict__[varname]  # a output_nn cannot be a list
                odir = os.path.dirname(fname)
                f.write("mkdir -p " + odir + "\n")

        f.write(self.command())

        f.write('\n')
        f.close()


def touch(fname):
    open(fname, 'w').close()


def conditional(value, param):  # conditional(argument.run, "--run")
    if value:
        return str(param)
    else:
        return ""


def optional(param, value):  # optional("-V", argument.myvcf)
    if value:
        return " {}{} ".format(param, value)
    else:
        return ""


def repeat(param, values):  # repeat("INPUT=", input.bamsToMerge)
    retstr = ""
    for value in values:
        retstr += " {}{}".format(param, value)
    return retstr + " "


def required(param, value):  # required("-b ", self.algorithm) ==> -b bwtsw
    if value is None:
        raise ValueError("parameter {} is required".format(param))
    retstr = " {}{} ".format(param, value)
    return retstr


def stripsuffix(thestring, suffix):
    if thestring.endswith(suffix):
        return thestring[:-len(suffix)]
    return thestring


def mkdir(dir_to_make):
    """ Create a directory if it doesn't exist
    :param dir_to_make: dir to create
    """
    if not os.path.isdir(dir_to_make):
        try:
            os.makedirs(dir_to_make)
        except OSError:
            logging.error("Couldn't create directory {}".format(dir_to_make))
    else:  # if dir already exists, do nothing
        pass
