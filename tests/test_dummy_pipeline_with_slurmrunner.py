import json
import os
import tempfile
import unittest

import sys

from pypedream.runners.slurmrunner import Slurmrunner
from pypedream.pipeline.dummy_pipeline import TestPipeline


class TestDummyPipeline(unittest.TestCase):
    p = None
    outdir = None

    def setUp(self):
        self.outdir = tempfile.mkdtemp()
        print sys.stderr, "Testing with Slurmrunner"
        self.p = TestPipeline(self.outdir, "first-slurm", "second-slurm", "third-slurm",
                              runner=Slurmrunner(interval=1),
                              jobdb="{}/jobs.json".format(self.outdir))

        print sys.stderr, "Starting"
        self.p.start()
        self.p.join()

    def test_output_exists(self):
        self.assertTrue(os.path.exists(self.outdir + "/third-slurm"))
        self.assertTrue(os.path.exists(self.outdir + "/.third-slurm.done"))

    def test_intermediate_is_deleted(self):
        self.assertTrue(not os.path.exists(self.outdir + "/second-slurm"))
        self.assertTrue(os.path.exists(self.outdir + "/third-slurm"))

    def test_starttime_and_endtimes_are_set(self):
        jobdb = json.load(open("{}/jobs.json".format(self.outdir)))
        jobs = jobdb['jobs']
        print str(jobs)
        self.assertIsNotNone(jobs[0]['starttime'])
        self.assertIsNotNone(jobs[0]['endtime'])