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
        runner = runners.shellrunner.Shellrunner()

        # act, pipeline should raise OSError when failing
        with self.assertRaises(OSError):
            runner.run(self.p)

        # assert, .fail file should be in place
        self.assertTrue(os.path.exists("/tmp/.second.fail"))
