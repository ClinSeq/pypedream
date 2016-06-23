import logging
import os
import uuid

import datetime

from pypedream import constants
from pypedream.pypedreamstatus import PypedreamStatus

__author__ = 'dankle'


class Job(object):
    """ A abstract class of a tool
    """
    jobid = None
    jobname = None
    starttime = None
    endtime = None
    threads = 1
    scratch = "/tmp"
    log = None
    script = None
    is_intermediate = False
    status = PypedreamStatus.PENDING

    def __init__(self):
        self.data = {}  # any additional data that the runner needs a job to keep track of

    def command(self):
        raise NotImplementedError("Class %s doesn't implement run()" % self.__class__.__name__)

    def get_name(self):
        if self.jobname:
            return self.jobname
        else:
            self.jobname = "clifunc" + str(abs(hash(self)))

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
        What makes a job unique is it's inputs and outputs, so hash a list of that.
        :return: hash of object
        """
        return hash(str([self.get_inputs(), self.get_outputs()]))

    def __str__(self):
        return self.get_name()

    @staticmethod
    def try_remove_files(files):
        for f in files:
            if os.path.exists(f):
                os.remove(f)

    @staticmethod
    def touch_files(files):
        for f in files:
            touch(f)

    def complete(self):
        self.status = PypedreamStatus.COMPLETED
        self.try_remove_files(self.failfiles())
        self.touch_files(self.donefiles())

    def fail(self):
        self.status = PypedreamStatus.FAILED
        self.try_remove_files(self.donefiles())
        self.touch_files(self.failfiles())

    def donefiles(self):
        donefiles = []
        for varname in self.__dict__:
            if varname.startswith(constants.OUTPUT):
                fname = self.__dict__[varname]  # a output_nn cannot be a list
                odir = os.path.dirname(fname)
                obase = os.path.basename(fname)
                donefiles.append("{}/.{}.done".format(odir, obase))
        return donefiles

    def failfiles(self):
        failfiles = []
        for varname in self.__dict__:
            if varname.startswith(constants.OUTPUT):
                fname = self.__dict__[varname]  # a output_nn cannot be a list
                odir = os.path.dirname(fname)
                obase = os.path.basename(fname)
                failfiles.append("{}/.{}.fail".format(odir, obase))
        return failfiles

    def all_donefiles_exists(self):
        return all([os.path.exists(f) for f in self.donefiles()])

    def write_script(self, script_dir, pipeline):
        logging.debug("Writing script for task " + self.get_name())
        hashes = [hash(job) for job in pipeline.get_ordered_jobs()]
        idx = hashes.index(hash(self))

        logging.debug("Task index is " + str(idx))
        self.script = "{dir}/{name}__{idx}__{uuid}.sh".format(dir=script_dir,
                                                              name=self.get_name().replace("/", "_"),
                                                              idx=idx,
                                                              uuid=uuid.uuid4())

        f = open(self.script, 'w')
        f.write("#!/usr/bin/env bash\n")
        f.write("set -eo pipefail\n")
        # f.write("set -eu\n")
        f.write("\n")

        # create directories for output files
        for varname in self.__dict__:
            if varname.startswith(constants.OUTPUT):
                fname = self.__dict__[varname]  # a output_nn cannot be a list
                odir = os.path.dirname(fname)
                f.write("mkdir -p " + odir + "\n")

        f.write("\n")
        f.write(self.command())
        f.write("\n")
        # f.write("OUT=$?\n")
        # f.write("\n")
        # f.write("if [ $OUT -eq 0 ];then\n")  # success
        # f.write(self.complete_bash() + "\n")
        # f.write("else\n")
        # f.write(self.fail_bash() + "\n")  # fail
        # f.write("fi\n")
        # f.write("\n")
        # f.write("exit $OUT\n")
        f.close()

    def complete_bash(self):
        s = []
        for f in self.failfiles():
            s.append("  if [ -f {} ]; then".format(f))
            s.append("    rm {}".format(f))
            s.append("  fi")
        for f in self.donefiles():
            s.append("  touch {}".format(f))
        return "\n".join(s)

    def fail_bash(self):
        s = []
        for f in self.donefiles():
            s.append("  if [ -f {} ]; then".format(f))
            s.append("    rm {}".format(f))
            s.append("  fi")
        for f in self.failfiles():
            s.append("  touch {}".format(f))
        return "\n".join(s)


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
    if type(values) is not list and values is not None:
        raise ValueError("Values must be a list. Single values must be wrapped.")
    retitems = []
    if values is None:
        return ""
    else:
        for value in values:
            retitems.append("{}{}".format(param, value))
        return " {} ".format(" ".join(retitems))


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
