import os
import sys

from invenio.refextract_api import extract_references_from_record_xml, \
                                   FullTextNotAvailable
from invenio.refextract_config import CFG_REFEXTRACT_VERSION
from invenio.search_engine import perform_request_search

AFS_HOME = '/afs/cern.ch/user/a/adeiana'
DEST_DIR = 'refextract-results/%s' % CFG_REFEXTRACT_VERSION[40:]


def main(force=False):
    recids = perform_request_search(p='', cc='HEP')

    # Create dest dir
    try:
        os.mkdir(os.path.join(AFS_HOME, DEST_DIR))
    except OSError, e:
        if e.errno != 17:
            raise

    for done, recid in enumerate(recids):

        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        filename = '%s.xml' % recid
        dest_path = os.path.join(AFS_HOME, DEST_DIR, filename)
        if not force and os.path.isfile(dest_path):
            continue

        try:
            xml = extract_references_from_record_xml(recid)
            print 'processed', recid
        except FullTextNotAvailable:
            continue

        out = open(dest_path, 'w')
        out.write(xml)

    print 'done'

if __name__ == '__main__':
    try:
        force = '-f' in sys.argv
        main(force=force)
    except KeyboardInterrupt:
        print 'Exiting'
