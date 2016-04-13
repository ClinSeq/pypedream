import os
import tempfile
import unittest

from pypedream.pipeline.dummy_pipeline_that_fails import FailingPipeline
from pypedream.runners.localqrunner import Localqrunner


class TestDummyPipelineThatFails(unittest.TestCase):
    p = None

    def test_fail_file_exists(self):
        # arrange
        outdir = tempfile.mkdtemp()
        self.p = FailingPipeline(outdir, "first", "second", "third", runner=Localqrunner(4))

        self.p.start()
        self.p.join()

        # act, pipeline should return != 0 when failing
        self.assertNotEqual(self.p.exitcode, 0,
                            "Pipeline exitcode should be nonzero when failing (got {})".format(self.p.exitcode))

        # assert, .fail file should be in place
        self.assertTrue(os.path.exists("/tmp/.second.fail"))
