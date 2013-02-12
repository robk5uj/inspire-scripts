# -*- coding: utf-8 -*-
import unittest
from doctest import Example

from lxml.doctestcompare import LXMLOutputChecker

from datacleaning.job_helper import get_record, \
                                    all_recids, \
                                    ChunkedTask, \
                                    BibRecord

from invenio.search_engine import get_record as get_record_original
from invenio.search_engine import perform_request_search
from invenio.bibrecord import print_rec


class XmlTest(unittest.TestCase):
    def assertXmlEqual(self, got, want):
        checker = LXMLOutputChecker()
        if not checker.check_output(want, got, 0):
            message = checker.output_difference(Example("", want), got, 0)
            raise AssertionError(message)


class BibRecordTest(XmlTest):
    def setUp(self):
        from invenio import bibrecord

        def order_by_tag(field1, field2):
            """Function used to order the fields according to their tag"""
            return cmp(field1[0], field2[0])
        bibrecord._order_by_ord = order_by_tag

        self.records_cache = {}
        self.xml_cache = {}
        for recid in perform_request_search(p=""):
            record = get_record(recid)
            self.records_cache[recid] = record
            self.xml_cache[recid] = record.to_xml()

    def test_get_record(self):
        for recid in perform_request_search(p=""):
            # Our bibrecord we want to test
            record = self.records_cache[recid]
            # Reference implementation
            original_record = get_record_original(recid)

            self.assertXmlEqual(record.to_xml(), print_rec(original_record))

    def test_equality(self):
        for recid in self.records_cache.iterkeys():
            for recid2 in self.records_cache.iterkeys():
                record = self.records_cache[recid]
                xml = self.xml_cache[recid]
                if recid == recid2:
                    record2 = get_record(recid)
                    xml2 = record2.to_xml()
                    self.assertEqual(record, record2)
                    self.assertXmlEqual(xml, xml2)
                else:
                    record2 = self.records_cache[recid2]
                    xml2 = self.xml_cache[recid2]
                    self.assertNotEqual(record, record2)

    def test_hash(self):
        for recid, original_record in self.records_cache.iteritems():
            # Our bibrecord we want to test
            record = BibRecord()

            for tag, fields in original_record.record.iteritems():
                record[tag] = list(set(fields))
                self.assertEqual(set(record[tag]), set(original_record[tag]))

            self.assertEqual(record, original_record)


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
