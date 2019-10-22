from pypedreampipeline import *
from pypedream.tools.unix import Cat, Urandom, Ifail

__author__ = 'dankle'


class FailingPipeline(PypedreamPipeline):
    def __init__(self, outdir, first, second, third, **kwargs):
        PypedreamPipeline.__init__(self, outdir, **kwargs)

        rnd1 = Urandom()
        rnd1.output = outdir + "/" + first
        self.add(rnd1)

        rnd2 = Ifail()
        rnd2.output = outdir + "/" + second
        self.add(rnd2)

        cat1 = Cat()
        cat1.input = [rnd1.output, rnd2.output]
        cat1.output = outdir + "/" + third
        self.add(cat1)


