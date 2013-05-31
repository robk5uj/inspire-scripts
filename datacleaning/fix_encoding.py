import sys

from invenio.search_engine import get_record as get_record_orig
from invenio.docextract_record import BibRecordControlField, \
                                      BibRecordField, \
                                      BibRecord, \
                                      BibRecordSubField, \
                                      get_record

from job_helper import ChunkedBibUpload, all_recids


SCRIPT_NAME = 'fix-encoding'


def convert_record(bibrecord):
    def create_control_field(inst):
        return BibRecordControlField(inst[3].decode('utf-8', 'ignore'))

    def create_field(inst):
        subfields = [BibRecordSubField(code, value.decode('utf-8', 'ignore'))
                                                for code, value in inst[0]]
        return BibRecordField(ind1=inst[1], ind2=inst[2], subfields=subfields)

    record = BibRecord()
    for tag, instances in bibrecord.iteritems():
        if tag.startswith('00'):
            record[tag] = [create_control_field(inst) for inst in instances]
        else:
            record[tag] = [create_field(inst) for inst in instances]

    return record


def fix_encoding(recid):
    return convert_record(get_record_orig(recid)).to_xml()


def main():
    bibupload = ChunkedBibUpload(mode='c', user=SCRIPT_NAME, notimechange=True)
    recids = sys.argv[1:]
    if '*' in recids:
        recids = all_recids()
    for recid in recids:
        try:
            get_record(recid)
        except UnicodeDecodeError:
            bibupload.add(fix_encoding(int(recid)))
    bibupload.cleanup()

if __name__ == '__main__':
    main()
