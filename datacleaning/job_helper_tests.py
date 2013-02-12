# -*- coding: utf-8 -*-
import unittest
from doctest import Example

from lxml.doctestcompare import LXMLOutputChecker

from job_helper import get_record
from invenio.search_engine import get_record as get_record_original
from invenio.bibrecord import print_rec


class XmlTest(unittest.TestCase):
    def assertXmlEqual(self, got, want):
        checker = LXMLOutputChecker()
        if not checker.check_output(want, got, 0):
            message = checker.output_difference(Example("", want), got, 0)
            raise AssertionError(message)


class BibRecordTest(XmlTest):

    def test_get_record(self):
        record = get_record(1)
        self.assertXmlEqual(record.to_xml(), print_rec(get_record_original(1)))
