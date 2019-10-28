import os
import tempfile
import unittest

import time

from pypedream.pypedreamstatus import PypedreamStatus

from pypedream import runners
from pypedream.pipeline.dummy_pipeline_that_fails import FailingPipeline
from pypedream.runners.shellrunner import Shellrunner


class TestDummyPipelineThatFails(unittest.TestCase):
    p = None

    def test_fail_file_exists(self):
        # arrange
        outdir = tempfile.mkdtemp()
        self.p = FailingPipeline(outdir, "first", "second", "third", runner=Shellrunner())

        self.p.start()
        self.p.join()

        # act, pipeline should return != 0 when failing
        self.assertNotEqual(self.p.exitcode, 0,
                            'Pipeline exitcode should be non-zero when failing (got {})'.format(self.p.exitcode))

        self.assertEqual(self.p.exitcode, 127,
                            'Pipeline exitcode should be 127 with this error(got {})'.format(self.p.exitcode))

        # assert, .fail file should be in place
        self.assertTrue(os.path.exists(os.path.join(outdir, ".second.fail")))
