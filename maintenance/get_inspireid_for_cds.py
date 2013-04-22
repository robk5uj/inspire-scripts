#!/usr/bin/env python

"""
This script is going to be used in order to populate records in CDS with
INSPIRE IDs by matching them via DOI or arXiv.
In this way we can have a clear idea of what is INSPIRE already.

The output is

INSPIREID---arXiv---DOI
"""

from invenio.search_engine import perform_request_search, get_record
from invenio.bibrecord import record_get_field_instances, field_get_subfield_instances
from invenio.intbitset import intbitset

recids = intbitset(perform_request_search(p='037__9:"arxiv" or 0247_2:"DOI"', cc='HEP'))

for recid in recids:
    arxiv = ''
    doi = ''
    record = get_record(recid)
    for field in record_get_field_instances(record, '037', '_', '_'):
        in_arxiv = False
        possible_arxiv = ''
        for (code, value) in field_get_subfield_instances(field):
            if code == '9' and value.lower().strip() == 'arxiv':
                in_arxiv = True
            elif code == 'a':
                possible_arxiv = value.strip()
        if in_arxiv and possible_arxiv:
            arxiv = possible_arxiv
            break
    for field in record_get_field_instances(record, '024', '7', '_'):
        in_doi = False
        possible_doi = ''
        for (code, value) in field_get_subfield_instances(field):
            if code == '2' and value.lower().strip() == 'doi':
                in_doi = True
            elif code == 'a':
                possible_doi = value.strip()
        if in_doi and possible_doi:
            doi = possible_doi
            break
    print '%s---%s---%s' % (recid, arxiv, doi)




