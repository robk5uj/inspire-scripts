from ids import SELECTED_IDS
from invenio.search_engine import get_record
from invenio.bibrecord import print_rec

DEST_FILE = 'out.xml'


def main(recids):
    out = open(DEST_FILE, 'w')

    for done, recid in enumerate(recids):

        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        xml = print_rec(get_record(recid))
        out.write(xml.encode('utf-8'))

    out.close()
    print 'done'


if __name__ == '__main__':
    try:
        main(SELECTED_IDS)
    except KeyboardInterrupt:
        print 'Exiting'
