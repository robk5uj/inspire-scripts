from job_helper import ChunkedBibUpload, \
                       ChunkedBibIndex, \
                       loop
from invenio.docextract_record import get_record, \
                                      BibRecord, \
                                      BibRecordField, \
                                      BibRecordSubField
from invenio.search_engine import perform_request_search

SCRIPT_NAME = 'lisbon-aff-repl'


def create_our_record(recid):
    old_record = get_record(recid)

    for subfield in old_record.find_subfields('100__u'):
        if subfield.value.lower() == 'lisbon, lifep':
            subfield.value = 'LIP, Lisbon'

    for subfield in old_record.find_subfields('700__u'):
        if subfield.value.lower() == 'lisbon, lifep':
            subfield.value = 'LIP, Lisbon'

    try:
        instances_100 = old_record['100']
    except KeyError:
        instances_100 = []

    try:
        instances_700 = old_record['700']
    except KeyError:
        instances_700 = []

    record = BibRecord(recid=recid)
    record['100'] = instances_100
    record['700'] = instances_700
    return record.to_xml()


def main():
    bibupload = ChunkedBibUpload(mode='c', user=SCRIPT_NAME, notimechange=True)
    bibindex = ChunkedBibIndex(indexes='author', user=SCRIPT_NAME)

    def cb_process_one(recid):
        xml = create_our_record(recid)
        bibupload.add(xml)
        bibindex.add(recid)

    recids = perform_request_search(p='700__u:"lisbon, lifep" or 100__u:"lisbon, LIFEP"')
    loop(recids, cb_process_one)


if __name__ == '__main__':
    main()
