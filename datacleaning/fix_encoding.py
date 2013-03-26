import sys

from invenio.search_engine import get_record
from invenio.docextract_record import BibRecordControlField, \
                                      BibRecordField, \
                                      BibRecord, \
                                      BibRecordSubField

from job_helper import ChunkedBibUpload


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
    return convert_record(get_record(recid)).to_xml()


def main():
    bibupload = ChunkedBibUpload(mode='c', user=SCRIPT_NAME, notimechange=True)
    for recid in sys.argv[1:]:
        bibupload.add(fix_encoding(int(recid)))


if __name__ == '__main__':
    main()
