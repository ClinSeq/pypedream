import os
import tempfile
import unittest

from pypedream.pipeline.dummy_pipeline_that_fails import FailingPipeline
from pypedream.runners.slurmrunner import Slurmrunner


class TestDummyPipelineThatFails(unittest.TestCase):
    p = None

    def test_fail_file_exists(self):
        # arrange
        outdir = tempfile.mkdtemp()
        self.p = FailingPipeline(outdir, "first-slurm-testfail", "second-slurm-testfail",
                                 "third-slurm-testfail", runner=Slurmrunner(interval=1),
                                 jobdb="{}/jobs.db".format(outdir))

        self.p.start()
        self.p.join()

        # act, pipeline should return != 0 when failing
        self.assertNotEqual(self.p.exitcode, 0,
                            "Pipeline exitcode should not be non-zero when failing (got {})".format(self.p.exitcode))

        # assert, .fail file should be in place
        self.assertTrue(os.path.exists(os.path.join(outdir, ".second-slurm-testfail.fail")))
