#!/usr/bin/env python

import sys
import re

from invenio.search_engine import perform_request_search
from invenio.config import CFG_CERN_SITE
from invenio.bibrecord import record_add_field
from invenio.bibrecord import record_xml_output

_RE_PARSE_ANNETTE_FILE = re.compile("^(?P<rna>.+?)\s+\d+\s+(?P<rnb>.+?)$")

def rn2recid(rn):
    if CFG_CERN_SITE:
        collection = "Articles & Preprints"
    else:
        collection = "HEP"
    recids = perform_request_search(cc=collection, p=rn, f="reportnumber")
    if len(recids) != 1:
        raise ValueError("Not exactly one and only one recid matched '%s': %s" % (rn, recids))
    return recids[0]

def supersedes(rn1, rn2):
    """
    Get the MARCXML necessary to declare that rn1 supersedes rn2 and
    consequently rn2 is superseded by rn1.
    """
    recid1 = rn2recid(rn1)
    recid2 = rn2recid(rn2)

    rec1 = {}
    record_add_field(rec1, tag="001", controlfield_value=str(recid1))
    record_add_field(rec1, tag="780", ind1="0", ind2="2", subfields=[('i', 'supersedes'), ('r', rn2), ('w', str(recid2))])

    rec2 = {}
    record_add_field(rec2, tag="001", controlfield_value=str(recid2))
    record_add_field(rec2, tag="785", ind1="0", ind2="2", subfields=[('i', 'superseded by'), ('r', rn1), ('w', str(recid1))])

    return record_xml_output(rec1) + record_xml_output(rec2)

def main():
    print "<collection>"
    for i, line in enumerate(sys.stdin):
        line = line.strip()
        if not line:
            continue
        g = _RE_PARSE_ANNETTE_FILE.search(line)
        if g:
            rna = g.group('rna')
            rnb = g.group('rnb')
        else:
            print >> sys.stderr, "Can't reliably parse line %s '%s'" % (i + 1, line)
            continue
        try:
            print supersedes(rnb, rna)
        except ValueError, err:
            print >> sys.stderr, "Can't automatically elaborate line %s '%s': %s" % (i + 1, line, err)
    print "</collection>"

if __name__ == "__main__":
    main()
