import sys

from invenio.search_engine import perform_request_search
from invenio.search_engine import get_record
from invenio.bibrecord import print_rec

DEST_FILE = 'out.xml'


def main(recids):
    out = open(DEST_FILE, 'w')

    for done, recid in enumerate(recids):

        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        xml = print_rec(get_record(recid))
        out.write(xml)

    out.close()
    print 'done'


def usage():
    print >>sys.stderr, """Usage: python save_records.py "collection:HEP" """
    sys.exit(1)


if __name__ == '__main__':
    try:
        try:
            p = sys.argv[1]
        except IndexError:
            usage()
        recids = perform_request_search(p=p, cc='HEP')
        main(recids)
    except KeyboardInterrupt:
        print 'Exiting'
