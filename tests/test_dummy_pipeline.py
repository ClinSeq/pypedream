import json
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
                              jobdb="{}/jobs.json".format(self.outdir), runner=Shellrunner())

        self.p.start()
        self.p.join()

    def test_output_exists(self):
        self.assertTrue(os.path.exists(self.outdir + "/third"))
        self.assertTrue(os.path.exists(self.outdir + "/.third.done"))

    def test_intermediate_is_deleted(self):
        self.assertTrue(not os.path.exists(self.outdir + "/second"))
        self.assertTrue(os.path.exists(self.outdir + "/third"))

    def test_starttime_and_endtimes_are_set(self):
        jobdb = json.load(open("{}/jobs.json".format(self.outdir)))
        jobs = jobdb['jobs']
        print str(jobs)
        self.assertIsNotNone(jobs[0]['starttime'])
        self.assertIsNotNone(jobs[0]['endtime'])

    def test_jobdb_has_jobs_with_names_input_and_output(self):
        jobdb = json.load(open("{}/jobs.json".format(self.outdir)))
        jobs = jobdb['jobs']
        jobnames = [j['jobname'] for j in jobs]

        job_name_to_find = "cat1-third"

        self.assertIn(job_name_to_find, jobnames)

        idx = jobnames.index(job_name_to_find)
        self.assertIn("{}/first".format(self.outdir), jobs[idx]["inputs"]["input"])
        self.assertIn("{}/second".format(self.outdir), jobs[idx]["inputs"]["input"])

        self.assertEqual(jobs[idx]["outputs"]["output"], "{}/third".format(self.outdir))

