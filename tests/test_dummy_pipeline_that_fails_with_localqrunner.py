import os
import unittest
from pypedream import runners
from pypedream.pipeline import dummy_pipeline_that_fails


class TestDummyPipelineThatFails(unittest.TestCase):
    p = None

    def test_fail_file_exists(self):
        # arrange
        self.p = dummy_pipeline_that_fails.Pipeline("/tmp", "first", "second", "third")
        self.p.add_edges()
        runner = runners.localqrunner.Localqrunner(4)

        # act, pipeline should return != 0 when failing
        self.assertNotEqual(runner.run(self.p), 0, "Pipeline returncode should not be 0 when failing")

        # assert, .fail file should be in place
        self.assertTrue(os.path.exists("/tmp/.second.fail"))
