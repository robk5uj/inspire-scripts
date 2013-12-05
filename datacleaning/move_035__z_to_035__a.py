from job_helper import ChunkedBibUpload, \
                       ChunkedBibIndex, \
                       loop
from invenio.docextract_record import get_record, BibRecord
from invenio.search_engine import perform_request_search
from invenio.search_engine_utils import get_fieldvalues

SCRIPT_NAME = 'move-035z-texkey'


def create_our_record(recid, record):
    instances = record['035']
    new_record = BibRecord(recid=recid)
    new_record['035'] = instances
    return new_record.to_xml()

def needs_migration(recid):
    if get_fieldvalues(recid, '035__a'):
        return False
    return True
    # return any(v in ("SPIRESTeX", "INSPIRETeX") for v in get_fieldvalues(recid, '035__9'))


def mangle_record(record):
    for field in record['035']:
        if field['9'][0] == "SPIRESTeX": #has_inspire and field['9'][0] == "INSPIRETeX" or has_spires and field['9'][0] == "SPIRESTeX":
            for subfield in field.subfields:
                if subfield.code == 'z':
                    print 'changing subfield %s from z to a' % subfield.value
                    subfield.code = 'a'
                    return


def main():
    bibupload = ChunkedBibUpload(mode='c', user=SCRIPT_NAME, notimechange=True)
    bibindex = ChunkedBibIndex(indexes='global', name='global', user=SCRIPT_NAME)

    def cb_process_one(recid):
        if needs_migration(recid):
            record = get_record(recid)
            assert record['035__z']
            print 'processing', recid
            sources = set(subfield.value for subfield in record.find_subfields('035__9'))
            has_spires = "SPIRESTeX" in sources
            # has_inspire = "INSPIRETeX" in sources
            # assert has_inspire or has_spires
            assert has_spires
            mangle_record(record)
            bibupload.add(create_our_record(recid, record))
            bibindex.add(recid)

    # recids = perform_request_search(p='035__z:/.+/', of='intbitset')
    # recids = perform_request_search(p='035__9:INSPIRETeX -035__a:/.+/', of='intbitset')
    recids = perform_request_search(p='035__9:SPIRESTeX -035__a:/.+/', of='intbitset')
    loop(recids, cb_process_one, step=20000)


if __name__ == '__main__':
    main()
