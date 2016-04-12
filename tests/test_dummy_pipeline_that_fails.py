import os
import unittest

from pypedream.pypedreamstatus import PypedreamStatus

from pypedream import runners
from pypedream.pipeline.dummy_pipeline_that_fails import FailingPipeline
from pypedream.runners.shellrunner import Shellrunner


class TestDummyPipelineThatFails(unittest.TestCase):
    p = None

    def test_fail_file_exists(self):
        # arrange
        self.p = FailingPipeline("/tmp", "first", "second", "third", runner=Shellrunner())

        self.p.start()
        self.p.join()

        self.p.add_edges()
        runner = runners.shellrunner.Shellrunner()

        # act, pipeline should return != 0 when failing
        self.assertEqual(self.p.status, PypedreamStatus.FAILED,
                         'Pipeline status should not be FAILED when failing (got {})'.format(self.p.status))

        # assert, .fail file should be in place
        self.assertTrue(os.path.exists("/tmp/.second.fail"))
