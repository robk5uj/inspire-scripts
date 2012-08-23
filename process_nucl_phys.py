from tempfile import mkstemp
import os
import time

from invenio.config import CFG_TMPDIR
from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission
from invenio.search_engine import get_record
from invenio.search_engine_utils import get_fieldvalues
from invenio.bibrecord import print_rec, \
                              record_get_field_instances, \
                              record_add_field, \
                              record_add_fields, \
                              field_get_subfield_instances

JOURNAL_TITLE = 'Nucl.Phys.Proc.Suppl.'
PREFIX = 'nucl-phys-proc-suppl-b'


def submit_task(to_submit, mode):
    # Save new record to file
    (temp_fd, temp_path) = mkstemp(prefix=PREFIX,
                                   dir=CFG_TMPDIR)
    temp_file = os.fdopen(temp_fd, 'w')
    temp_file.write('<?xml version="1.0" encoding="UTF-8"?>')
    temp_file.write('<collection>')
    for el in to_submit:
        temp_file.write(el)
    temp_file.write('</collection>')
    temp_file.close()

    return task_low_level_submission('bibupload', PREFIX, '-P', '3',
                                     '-%s' % mode, temp_path, '--notimechange')


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def create_xml(recid):
    old_record = get_record(recid)
    fields_to_add = record_get_field_instances(old_record,
                                               tag='999',
                                               ind1='%',
                                               ind2='%')
    for field in fields_to_add:
        subfields_list = field_get_subfield_instances(field)
        for index, subfield in enumerate(subfields_list):
            if subfield[0] == 's' and subfield[1].startswith(JOURNAL_TITLE):
                new_value = subfield[1]
                new_value = new_value.replace('%s,B' % JOURNAL_TITLE,
                                              '%s,' % JOURNAL_TITLE)
                new_value = new_value.replace('%s,b' % JOURNAL_TITLE,
                                              '%s,' % JOURNAL_TITLE)
                subfields_list[index] = ('s', new_value)

    record = {}
    record_add_field(record, '001', controlfield_value=str(recid))
    record_add_fields(record, '999', fields_to_add)
    return print_rec(record)


def main():
    to_process = []

    max_id = run_sql("SELECT max(id) FROM bibrec")[0][0]
    recids = xrange(1, max_id + 1)

    for done, recid in enumerate(recids):
        journals_refs = get_fieldvalues(recid, '999C5s')
        if [True for ref in journals_refs if ref.startswith('%s,B' % JOURNAL_TITLE) or ref.startswith('%s,b' % JOURNAL_TITLE)]:
            print 'processing %s' % recid
            xml = create_xml(recid)
            to_process.append(xml)

        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        if len(to_process) == 1000 or done + 1 == len(recids) and len(to_process) > 0:
            task_id = submit_task(to_process, 'c')
            print 'submitted task id %s' % task_id
            wait_for_task(task_id)
            to_process = []


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print 'Exiting'
