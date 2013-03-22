from job_helper import ChunkedBibUpload, \
                       ChunkedBibIndex, \
                       loop
from invenio.docextract_record import get_record, \
                                      BibRecord, \
                                      BibRecordField, \
                                      BibRecordSubField
from invenio.search_engine import perform_request_search

SCRIPT_NAME = 'move-65017-to-084__'


def create_our_record(recid, bibupload, bibupload2):
    old_record = get_record(recid)

    try:
        instances_084 = old_record['084']
    except KeyError:
        instances_084 = []

    to_remove_instances_650 = []


    modified = False
    for field in old_record['650']:
        if 'PACS' in field.get_subfield_values('2'):
            assert len(field.subfields) >= 2
            assert len(field.subfields) -1 == len(field.get_subfield_values('a'))
            to_remove_instances_650.append(field)
            for value in field.get_subfield_values('a'):
                sub_2 = BibRecordSubField(code='2', value='PACS')
                sub_a = BibRecordSubField(code='a', value=value)
                f = BibRecordField(subfields=[sub_2, sub_a])
                instances_084.append(f)
                modified = True

    if not modified:
        return None

    # Remove wrong indicator
    for field in instances_084[:]:
        if field.ind1 == '7' and field.ind2 == ' ' \
                and 'PACS' in field.get_subfield_values('2'):
            field.ind1 = ' '
            field.ind2 = ' '

    record = BibRecord(recid=recid)
    record['084'] = set(instances_084)
    bibupload.add(record.to_xml())

    if to_remove_instances_650:
        record = BibRecord(recid=recid)
        record['650'] = to_remove_instances_650
        bibupload2.add(record.to_xml())


def main():
    bibupload = ChunkedBibUpload(mode='c', user=SCRIPT_NAME, notimechange=True)
    bibupload2 = ChunkedBibUpload(mode='d', user=SCRIPT_NAME, notimechange=True)
    bibindex = ChunkedBibIndex(indexes='subject', user=SCRIPT_NAME)

    def cb_process_one(recid):
        create_our_record(recid, bibupload, bibupload2)
        bibindex.add(recid)

    #recids = perform_request_search(p='650172:pacs')
    recids = perform_request_search(p='6507_2:pacs')
    loop(recids, cb_process_one)


if __name__ == '__main__':
    main()
