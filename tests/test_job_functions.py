import unittest
from pypedream import job


class TestCommandFunctions(unittest.TestCase):
    def test_conditional(self):
        #  conditional(argument.run, "--run")
        # test that True yields the parameter without spaces
        res = job.conditional(True, "-a")
        self.assertEqual(res, "-a")

        # test that False yields an empty string
        res = job.conditional(False, "-a")
        self.assertEqual(res, "")

        # test that an int is converted to a str
        res = job.conditional(True, 42)
        self.assertEqual(res, "42")

    def test_optional(self):
        #  optional("-V ", argument.myvcf) yields " -V file.vcf ", note padded spaces
        # test that a no space is added between param and value, but at start and end
        res = job.optional("-V ", "file.vcf")
        self.assertEqual(res, " -V file.vcf ")

        # Test that a value of None or empty string yields an empty string
        res = job.optional("-V ", None)
        self.assertEqual(res, "")

        res = job.optional("-V ", "")
        self.assertEqual(res, "")

        # tes that an int is converted to a str
        res = job.optional("-t ", 8)
        self.assertEqual(res, " -t 8 ")

    def test_repeat(self):
        # repeat("INPUT=", input.bamsToMerge) yields " INPUT=1.bam INPUT=2.bam ", note padded spaces
        # test that a normal case works
        res = job.repeat("INPUT=", ["1.bam", "2.bam"])
        self.assertEqual(res, " INPUT=1.bam INPUT=2.bam ")

        # test that values=None yields an empty string
        res = job.repeat("INPUT=", None)
        self.assertEqual(res, "")

        # test that it raises ValueError if values is of type str
        # The user should wrap a str in a list instead
        self.assertRaises(ValueError, job.repeat, "INPUT=", "1.bam")

    def test_required(self):
        # Required throws ValueError if no value is supplied, or if it's None
        # test normal case, pads spaces
        res = job.required("-i ", "1.bam")
        self.assertEqual(res, " -i 1.bam ")

        # test that is raises ValueError if value is None
        self.assertRaises(ValueError, job.required, "-i ", None)

    def test_stripsuffix(self):
        # test normal case, should strip .vcf suffix
        res = job.stripsuffix("file.vcf", ".vcf")
        self.assertEqual(res, "file")

        # test case with consecutive suffixes (like .vcf.gz), should strip .vcf.gz suffix
        res = job.stripsuffix("file.vcf.gz", ".vcf.gz")
        self.assertEqual(res, "file")

        # test that it doesn't do anything if the file doens't have the given suffix
        res = job.stripsuffix("file.vcf", ".bam")
        self.assertEqual(res, "file.vcf")

