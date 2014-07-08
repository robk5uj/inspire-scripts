from job_helper import ChunkedBibUpload, loop, all_recids
from invenio.docextract_record import get_record, BibRecord
from invenio.search_engine import get_collection_reclist
from invenio.intbitset import intbitset
from invenio.search_engine_utils import get_fieldvalues
from invenio.bibrank_citation_indexer import re_CFG_JOURNAL_PUBINFO_STANDARD_FORM_REGEXP_CHECK
from invenio.refextract_linker import (get_recids_matching_query,
                                       standardize_report_number)


SCRIPT_NAME = 'clean-999C50'

SUBFIELD_MAPPER = {
    'a': 'IDENTIFIER',
    'i': 'ISBN',
    'r': 'REPORTNUMBER',
    's': 'JOURNAL',
}


def find_reportnumber(reportnumber_string):
    reportnumber = standardize_report_number(reportnumber_string)
    return get_recids_matching_query(reportnumber, 'reportnumber')


def find_journal(journal_string):
    # check reference value to see whether it is well formed:
    if not re_CFG_JOURNAL_PUBINFO_STANDARD_FORM_REGEXP_CHECK.match(journal_string):
        return []

    return get_recids_matching_query(journal_string, 'journal')


def find_identifier(identifier_string):
    if identifier_string.startswith('hdl'):
        identifier_string = identifier_string[:4]
    if identifier_string.startswith('doi'):
        identifier_string = identifier_string[:4]
    return get_recids_matching_query(identifier_string, '0247_a')


def find_isbn(isbn_string):
    books_recids = get_collection_reclist('Books')
    recids = intbitset(get_recids_matching_query(isbn_string, 'isbn'))
    return list(recids & books_recids)


def find_book(citation_element):
    books_recids = get_collection_reclist('Books')
    search_string = citation_element['title']
    recids = intbitset(get_recids_matching_query(search_string, 'title'))
    recids &= books_recids

    if not recids:
        return []

    if len(recids) == 1:
        return recids

    if 'year' in citation_element:
        for recid in recids:
            year_tags = get_fieldvalues(recid, '269__c')
            for tag in year_tags:
                if tag == citation_element['year']:
                    return [recid]

    return []


def search_recid(subfield):
    if subfield.code in SUBFIELD_MAPPER:
        finder_type = SUBFIELD_MAPPER[subfield.code]
        finder = FINDERS[finder_type]
        recids = finder(subfield.value)
        if len(recids) == 1:
            return recids


def main():
    bibupload = ChunkedBibUpload(mode='c', user=SCRIPT_NAME, notimechange=True)

    def cb_process_one(recid):
        record = get_record(recid)
        new_record = BibRecord(recid=recid)
        new_record['999'] = record['999']
        for field in new_record['999C5']:
            if not field['0']:
                for subfield in field.subfields:
                    ref_recid = search_recid(subfield)
                    if ref_recid:
                        print 'found ref recid for', recid
                        field.add_subfield('0', str(ref_recid))
                        break

        bibupload.add(new_record.to_xml())

    recids = all_recids()
    loop(recids, cb_process_one, step=20000)


FINDERS = {
    'JOURNAL': find_journal,
    'REPORTNUMBER': find_reportnumber,
    'IDENTIFIER': find_identifier,
    # 'BOOK': find_book,
    'ISBN': find_isbn,
}


if __name__ == '__main__':
    main()
