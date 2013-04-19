"""
Move corporate authors from 710__a into 110__a

There are almost 5000 records with 710__a but no author or corporate author
710__a:/.*/ -100__a:/.*/ -110__a:/.*/
https://inspirehep.net/search?wl=0&p=710__a%3A%2F.*%2F+-100__a%3A%2F.*%2F+-110__a%3A%2F.*%2F&of=hb&action_search=Search

710__a should be moved to 110__a for these records
"""

from job_helper import ChunkedBibUpload, \
                       ChunkedBibIndex, \
                       loop
from invenio.docextract_record import get_record, \
                                      BibRecord
from invenio.search_engine import perform_request_search

SCRIPT_NAME = 'move-710a-to-110a'


def create_our_record(recid, bibupload, bibupload2):
    old_record = get_record(recid)

    try:
        instances_710 = old_record['710']
    except KeyError:
        return

    record = BibRecord(recid=recid)
    record['110'] = set(instances_710)
    bibupload.add(record.to_xml())

    record = BibRecord(recid=recid)
    record['710'] = set(instances_710)
    bibupload2.add(record.to_xml())

    return record.to_xml()


def main():
    bibupload = ChunkedBibUpload(mode='c', user=SCRIPT_NAME, notimechange=True)
    bibupload2 = ChunkedBibUpload(mode='d', user=SCRIPT_NAME, notimechange=True)
    bibindex = ChunkedBibIndex(indexes='author', user=SCRIPT_NAME)

    def cb_process_one(recid):
        create_our_record(recid, bibupload, bibupload2)
        bibindex.add(recid)

    recids = perform_request_search(p='710__a:/.*/ -100__a:/.*/ -110__a:/.*/')
    loop(recids, cb_process_one)

    bibupload.cleanup()
    bibindex.cleanup()

if __name__ == '__main__':
    main()
