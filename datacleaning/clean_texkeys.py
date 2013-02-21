from job_helper import ChunkedBibUpload, \
                       loop, \
                       get_record, \
                       BibRecord
from invenio.search_engine import perform_request_search

SCRIPT_NAME = 'clean-texkeys'


def main():
    bibupload = ChunkedBibUpload(mode='d', user=SCRIPT_NAME, notimechange=True)

    def cb_process_one(recid):
        record = get_record(recid)
        instances = record['035']

        for field in instances:
            if "SPIRESTeX" in field.get_subfield_values(code='9') \
               and field.get_subfield_values(code='z')[0].startswith(':'):
                    new_record = BibRecord(recid=recid)
                    new_record['035'] = [field]
                    bibupload.add(new_record.to_xml())

    recids = perform_request_search(p='035:spirestex 035__z::*')
    loop(recids, cb_process_one)


if __name__ == '__main__':
    main()
