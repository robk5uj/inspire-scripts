import sys

from invenio.search_engine import search_pattern
from invenio.refextract_api import update_references, \
                                   RecordHasReferences, \
                                   FullTextNotAvailable


verbose = '-v' in sys.argv

recids = search_pattern(p='arxiv', f='reportnumber')


for count, recid in enumerate(recids):
    if count % 1000 == 0:
        print 'done %s of %s' % (count, len(recids))

    if verbose:
        print 'processing', recid

    try:
        update_references(recid, overwrite=False)
        print 'processed', recid
    except RecordHasReferences:
        pass
    except FullTextNotAvailable:
        pass
    except Exception, e:
        print 'exception', repr(e)
