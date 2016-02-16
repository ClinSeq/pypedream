import os
import tempfile
import unittest

import sys

from pypedream import runners
from pypedream.pipeline import dummy_pipeline


class TestDummyPipeline(unittest.TestCase):
    p = None
    outdir = None

    def setUp(self):
        self.outdir = tempfile.mkdtemp()
        #sys.stderr.write(self.outdir + "\n")
        self.p = dummy_pipeline.Pipeline(self.outdir, "first", "second", "third")
        self.p.add_edges()
        runner = runners.localqrunner.Localqrunner(2)
        runner.run(self.p)

    def test_output_exists(self):
        self.assertTrue(os.path.exists(self.outdir+"/third"))
        self.assertTrue(os.path.exists(self.outdir+"/.third.done"))

    def test_intermediate_is_deleted(self):
        self.assertTrue(not os.path.exists(self.outdir+"/second"))
        self.assertTrue(os.path.exists(self.outdir+"/third"))
