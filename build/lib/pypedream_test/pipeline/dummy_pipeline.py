from pypedreampipeline import *
from pypedream.tools.unix import Cat, Urandom

__author__ = 'dankle'


class TestPipeline(PypedreamPipeline):
    def __init__(self, outdir, first, second, third, **kwargs):
        PypedreamPipeline.__init__(self, outdir, **kwargs)

        rnd1 = Urandom()
        rnd1.jobname = "urandom-{}".format(first)
        rnd1.output = outdir + "/" + first
        rnd1.threads = 1
        self.add(rnd1)

        rnd2 = Urandom()
        rnd2.jobname = "urandom-{}".format(second)
        rnd2.output = outdir + "/" + second
        rnd2.is_intermediate = True
        self.add(rnd2)

        cat1 = Cat()
        cat1.jobname = "cat1-{}".format(third)
        cat1.input = [rnd1.output, rnd2.output]
        cat1.output = outdir + "/" + third
        self.add(cat1)


