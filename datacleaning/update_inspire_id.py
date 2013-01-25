from tempfile import mkstemp
import os
import time

from invenio.config import CFG_TMPDIR
from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission
from invenio.search_engine import get_record, perform_request_search
from invenio.bibrecord import print_rec, \
                              record_add_field, \
                              record_get_field_instances, \
                              record_add_fields


SCRIPT_NAME = 'fix-inspire-id'


def submit_task(to_update):
    # Save new record to file
    temp_fd, temp_path = mkstemp(prefix=SCRIPT_NAME, dir=CFG_TMPDIR)
    temp_file = os.fdopen(temp_fd, 'w')
    temp_file.write('<?xml version="1.0" encoding="UTF-8"?>')
    temp_file.write('<collection>')
    for el in to_update:
        temp_file.write(el)
    temp_file.write('</collection>')
    temp_file.close()

    return task_low_level_submission('bibupload', SCRIPT_NAME, '-P', '5',
                                     '-c', temp_path)


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def mangle(code, value):
    if code == 'i' and value == 'INSPIRE-00044037':
        value = 'INSPIRE-00283861'
    return value


def create_our_record(recid):
    old_record = get_record(recid)
    instances = record_get_field_instances(old_record, '700')
    for field in instances:
        subfields = [(code, mangle(code, value)) for code, value in field[0]]
        del field[0][:]
        field[0].extend(subfields)

    record = {}
    record_add_field(record, '001', controlfield_value=str(recid))
    record_add_fields(record, '700', instances)
    return print_rec(record)


def main():
    to_update = []

    recids = perform_request_search(p="700:INSPIRE-00044037 atlas")
    for done, recid in enumerate(recids):

        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        print 'cleaning', recid
        xml = create_our_record(recid)
        to_update.append(xml)

        if len(to_update) == 1000 or done + 1 == len(recids) and len(to_update) > 0:
            task_id = submit_task(to_update)
            print 'submitted task id %s' % task_id
            wait_for_task(task_id)
            to_update = []


if __name__ == '__main__':
    main()
