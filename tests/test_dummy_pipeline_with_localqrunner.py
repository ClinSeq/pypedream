import os
import tempfile
import unittest

from pypedream.pipeline.dummy_pipeline import TestPipeline
from pypedream.runners.localqrunner import Localqrunner


class TestDummyPipeline(unittest.TestCase):
    p = None
    outdir = None

    def setUp(self):
        self.outdir = tempfile.mkdtemp()
        self.p = TestPipeline(self.outdir, "first-localq", "second-localq", "third-localq",
                              runner=Localqrunner(2), jobdb="{}/jobs.json".format(self.outdir))

        self.p.start()
        self.p.join()

    def test_output_exists(self):
        self.assertTrue(os.path.exists(self.outdir + "/third-localq"))
        self.assertTrue(os.path.exists(self.outdir + "/.third-localq.done"))

    def test_intermediate_is_deleted(self):
        self.assertTrue(not os.path.exists(self.outdir + "/second-localq"))
        self.assertTrue(os.path.exists(self.outdir + "/third-localq"))
