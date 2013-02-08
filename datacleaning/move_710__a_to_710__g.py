from job_helper import ChunkedBibUpload, \
                       ChunkedBibIndex, \
                       loop, \
                       get_record, \
                       BibRecord
from invenio.search_engine import perform_request_search

SCRIPT_NAME = 'move-710g'


def mangle(code):
    if code == 'a':
        code = 'g'
    return code

def create_our_record(recid):
    old_record = get_record(recid)
    instances = old_record['710']

    for field in instances:
        for subfield in field.subfields:
            if subfield.code == 'a':
                subfield.code = 'g'

    record = BibRecord(recid=recid)
    record['710'] = instances
    return record.to_xml()


def main():
    bibupload = ChunkedBibUpload(mode='c', user=SCRIPT_NAME, notimechange=True)
    bibindex = ChunkedBibIndex(indexes='collaboration', user=SCRIPT_NAME)

    def cb_process_one(recid):
        bibupload.add(create_our_record(recid))
        bibindex.add(recid)

    recids = perform_request_search(p='710__g:/.+/')
    loop(recids, cb_process_one)


if __name__ == '__main__':
    main()
