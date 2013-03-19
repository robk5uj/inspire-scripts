# -*- coding: utf-8 -*-
import unittest
from doctest import Example

from lxml.doctestcompare import LXMLOutputChecker

from datacleaning.job_helper import all_recids, \
                                    ChunkedTask


class XmlTest(unittest.TestCase):
    def assertXmlEqual(self, got, want):
        checker = LXMLOutputChecker()
        if not checker.check_output(want, got, 0):
            message = checker.output_difference(Example("", want), got, 0)
            raise AssertionError(message)


class MiscTest(unittest.TestCase):

    def setUp(self):
        self.counter = 0

        class FakeSubmitter(ChunkedTask):
            chunk_size = 5

            @staticmethod
            def submitter(to_submit):
                self.counter += len(to_submit)
                return 1

        self.submitter_class = FakeSubmitter

    def test_all_recids(self):
        self.assert_(all_recids())

    def test_chunked_small_chunk(self):
        self.counter = 0
        submitter = self.submitter_class()
        submitter.add(1)
        del submitter
        assert self.counter == 1

    def test_chunked(self):
        self.counter = 0
        submitter = self.submitter_class()
        for i in xrange(1, 7):
            submitter.add(i)
        del submitter
        assert self.counter == 6
