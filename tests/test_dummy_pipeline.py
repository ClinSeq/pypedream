import os
import tempfile
import unittest

import sys

from pypedream.runners.shellrunner import Shellrunner

from pypedream import runners
from pypedream.pipeline.dummy_pipeline import TestPipeline


class TestDummyPipeline(unittest.TestCase):
    p = None
    outdir = None

    def setUp(self):
        self.outdir = tempfile.mkdtemp()
        self.p = TestPipeline(self.outdir, "first", "second", "third",
                              jobdb="{}/jobs.db".format(self.outdir), runner=Shellrunner())

        self.p.start()
        self.p.join()

    def test_output_exists(self):
        self.assertTrue(os.path.exists(self.outdir + "/third"))
        self.assertTrue(os.path.exists(self.outdir + "/.third.done"))

    def test_intermediate_is_deleted(self):
        self.assertTrue(not os.path.exists(self.outdir + "/second"))
        self.assertTrue(os.path.exists(self.outdir + "/third"))
