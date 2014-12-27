#!/usr/bin/env python

"""
By providing a pattern on the command line, it will re-harvest authorlist for
the matched records (in the HEP collection)
"""

import sys
import os
from os.path import join
from tempfile import mkstemp


from invenio.search_engine import perform_request_search
from invenio.bibformat_engine import format_record
from invenio.oai_harvest_daemon import call_authorlist_extract
from invenio.config import CFG_TMPSHAREDDIR
from invenio.bibtask import task_low_level_submission

CFG_QUEUE = "Authors"
CFG_STYLESHEET = "authorlist2marcxml.xsl"
CFG_SOURCE_ID = 3

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


recids = perform_request_search(p=sys.argv[1])
print "Retrieving authors for %s: %s records matching" % (sys.argv[1], len(recids))

for recids_chunk in chunks(recids, 50):
    print "Working on %s" % list(recids_chunk)
    fd, name = mkstemp(".xml", "tmp_catchup_authorlist_", CFG_TMPSHAREDDIR)
    print "Creating input file %s" % name
    output = os.fdopen(fd, "w")
    print >> output, "<collection>"
    for recid in recids_chunk:
        print >> output, format_record(recid, "xm")
    print >> output, "</collection>"
    output.close()
    authored_name = name + '.authors'
    print "Extracting authors for %s" % name
    call_authorlist_extract(name, authored_name, {}, CFG_QUEUE, CFG_STYLESHEET, CFG_SOURCE_ID)
    print "Scheduling upload for %s" % authored_name
    task_low_level_submission('bibupload', 'catchup_authorlist', "-c", authored_name)

print "DONE."
