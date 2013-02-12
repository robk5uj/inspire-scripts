from job_helper import ChunkedBibUpload, \
                       loop, \
                       get_record, \
                       BibRecord, \
                       all_recids
from invenio.search_engine import get_fieldvalues

SCRIPT_NAME = 'duplicate-abstract-fixup'


def mangle(code):
    if code == 'a':
        code = 'g'
    return code

def create_our_record(recid):
    old_record = get_record(recid)
    instances = old_record['520']

    record = BibRecord(recid=recid)
    record['520'] = set(instances)
    return record.to_xml()


def main():
    bibupload = ChunkedBibUpload(mode='c', user=SCRIPT_NAME, notimechange=True)

    def cb_process_one(recid):
        tags_520 = get_fieldvalues(recid, '520__a')
        if len(tags_520) > len(set(tags_520)):
            print 'processing %s' % recid
            bibupload.add(create_our_record(recid))

    loop(all_recids(), cb_process_one)


if __name__ == '__main__':
    main()
