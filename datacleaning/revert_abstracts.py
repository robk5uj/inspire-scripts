from tempfile import mkstemp
import os
import time
from invenio.config import CFG_TMPDIR

from invenio.bibedit_utils import get_record_revision_ids
from invenio.bibeditcli import get_marcxml_of_revision_id
from invenio.bibrecord import create_record, \
                              record_get_field_instances, \
                              record_add_fields, \
                              record_add_field, \
                              print_rec
from invenio.search_engine_utils import get_fieldvalues
from invenio.bibtask import task_low_level_submission
from invenio.dbquery import run_sql


def submit_task(to_submit, mode):
    # Save new record to file
    (temp_fd, temp_path) = mkstemp(prefix='duplicate-abstract-fixup',
                                   dir=CFG_TMPDIR)
    temp_file = os.fdopen(temp_fd, 'w')
    temp_file.write('<?xml version="1.0" encoding="UTF-8"?>')
    temp_file.write('<collection>')
    for el in to_submit:
        temp_file.write(el)
    temp_file.write('</collection>')
    temp_file.close()

    return task_low_level_submission('bibupload', 'duplicate-abstract-fixup',
                                     '-P', '3', '-%s' % mode, temp_path)


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def load_ids():
    f = open('to_process_cleaned.txt')
    for line in f:
        if line.strip():
            yield int(line)


def create_our_record(recid, abstracts):
    record = {}
    record_add_field(record, '001', controlfield_value=str(recid))
    record_add_fields(record, '520', abstracts[:1])
    return print_rec(record)


def rollback_record(recid):
        print 'id', recid
        for rev in get_record_revision_ids(recid):
            old_record = create_record(get_marcxml_of_revision_id(rev))
            fields_to_add = record_get_field_instances(old_record[0], tag='520')
            if fields_to_add:
                print 'reverting to', rev
                return create_our_record(recid, fields_to_add)
        print 'FAILED', recid


def main():
    to_process = []

    recids = list(load_ids())

    for done, recid in enumerate(recids):
        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        if not get_fieldvalues(recid, '520__a'):
            xml = rollback_record(recid)
            if xml:
                to_process.append(xml)

        if len(to_process) == 1000 or done + 1 == len(recids) and len(to_process) > 0:
            task_id = submit_task(to_process, 'a')
            print 'submitted task id %s' % task_id
            wait_for_task(task_id)
            to_process = []


if __name__ == '__main__':
    main()
