from pypedream.job import Job, repeat, required


class Cat(Job):
    """
    Concatenate files
    """
    def __init__(self):
        Job.__init__(self)
        self.input = None
        self.output = None
        self.jobname = "cat"

    def command(self):
        return "cat" + repeat("", self.input) + ">" + required("", self.output)


class Urandom(Job):
    """
    Concatenate files
    """
    def __init__(self):
        Job.__init__(self)
        self.output = None
        self.jobname = "urandom"

    def command(self):
        return "head /dev/urandom > " + required("", self.output)


class Ifail(Job):
    """
    fail
    """
    def __init__(self):
        Job.__init__(self)
        self.output = None
        self.jobname = "tool-that-fails"

    def command(self):
        return "blark-fail > " + required("", self.output)
