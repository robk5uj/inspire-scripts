from job_helper import ChunkedBibUpload, \
                       ChunkedBibIndex, \
                       loop
from invenio.search_engine import perform_request_search, \
                                  get_record
from invenio.bibrecord import print_rec, \
                              record_add_field, \
                              record_add_fields, \
                              record_get_field_instances

SCRIPT_NAME = 'move-710g'


def mangle(code):
    if code == 'a':
        code = 'g'
    return code

def create_our_record(recid):
    old_record = get_record(recid)
    instances = record_get_field_instances(old_record, '710')

    for field in instances:
        subfields = [(mangle(code), value) for code, value in field[0]]
        del field[0][:]
        field[0].extend(subfields)

    record = {}
    record_add_field(record, '001', controlfield_value=str(recid))
    record_add_fields(record, '710', instances)
    return print_rec(record)


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
