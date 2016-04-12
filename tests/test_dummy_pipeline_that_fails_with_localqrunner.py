import os
import unittest

from pypedream.pipeline import dummy_pipeline_that_fails
from pypedream.pypedreamstatus import PypedreamStatus
from pypedream.runners.localqrunner import Localqrunner


class TestDummyPipelineThatFails(unittest.TestCase):
    p = None

    def test_fail_file_exists(self):
        # arrange
        self.p = dummy_pipeline_that_fails.FailingPipeline("/tmp", "first", "second", "third", runner=Localqrunner(4))

        self.p.start()
        self.p.join()

        # act, pipeline should return != 0 when failing
        self.assertEqual(self.p.status, PypedreamStatus.FAILED,
                         "Pipeline status should not be FAILED when failing (got {})".format(self.p.status))

        # assert, .fail file should be in place
        self.assertTrue(os.path.exists("/tmp/.second.fail"))
