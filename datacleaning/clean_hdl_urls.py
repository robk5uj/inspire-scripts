from job_helper import ChunkedBibUpload, \
                       ChunkedBibIndex, \
                       loop
from invenio.docextract_record import get_record, BibRecord
from invenio.search_engine import perform_request_search
from invenio.search_engine_utils import get_fieldvalues

SCRIPT_NAME = 'clean-hdl'


def main():
    bibupload = ChunkedBibUpload(mode='r', user=SCRIPT_NAME)

    def cb_process_one(recid):
        record = get_record(recid)
        hdls = []
        # fields = record['856']
        # for field in fields:
        #     for subfield in field.subfields:
        #         if subfield.code == '2' and subfield.value == 'HDL':
        #             fields.remove(field)
        #             hdls.append(subfield.value)

        # record['856'] = fields

        for field in record['024']:
            for subfield in field.subfields:
                if subfield.code == 'a' and subfield.value.startswith('http://hdl.handle.net'):
                    subfield.value = subfield.value.replace('http://hdl.handle.net/', '')

        # # new_record = BibRecord(recid=recid)
        # for hdl in hdls:
        #     field = new_record.add_field('0247_')
        #     field.add_subfield('2', 'HDL')
        #     field.add_subfield('a', hdl)

        # print record.to_xml()
        bibupload.add(record.to_xml())

    recids = perform_request_search(p='024:"http://hdl.handle.net/*"')
    loop(recids, cb_process_one, step=20000)


if __name__ == '__main__':
    main()
