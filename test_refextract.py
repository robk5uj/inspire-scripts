import os

from ids import SELECTED_IDS
from invenio.refextract_api import extract_references_from_record_xml, \
                                   FullTextNotAvailable
from invenio.refextract_config import CFG_REFEXTRACT_VERSION

AFS_HOME = '/afs/cern.ch/user/a/adeiana'
AFS_HOME = '/Users/osso'
DEST_DIR = 'refextract-results/%s' % CFG_REFEXTRACT_VERSION[36:]


def main(recids):
    # Create dest dir
    try:
        os.mkdir(os.path.join(AFS_HOME, DEST_DIR))
    except OSError, e:
        if e.errno != 17:
            raise

    for done, recid in enumerate(recids):

        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        try:
            xml = extract_references_from_record_xml(recid)
            print 'processed', recid
        except FullTextNotAvailable:
            continue

        filename = '%s.xml' % recid
        out = open(os.path.join(AFS_HOME, DEST_DIR, filename), 'w')
        out.write(xml.encode('utf-8'))

    print 'done'

if __name__ == '__main__':
    try:
        main(SELECTED_IDS)
    except KeyboardInterrupt:
        print 'Exiting'
