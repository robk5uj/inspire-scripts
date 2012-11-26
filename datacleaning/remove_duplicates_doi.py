from tempfile import mkstemp
import os
import time

from invenio.config import CFG_TMPDIR
from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission
from invenio.search_engine_utils import get_fieldvalues
from invenio.bibrecord import print_rec, \
                              record_add_field


def submit_task(to_submit, mode):
    # Save new record to file
    (temp_fd, temp_path) = mkstemp(prefix='remove-duplicate-024',
                                   dir=CFG_TMPDIR)
    temp_file = os.fdopen(temp_fd, 'w')
    temp_file.write('<?xml version="1.0" encoding="UTF-8"?>')
    temp_file.write('<collection>')
    for el in to_submit:
        temp_file.write(el)
    temp_file.write('</collection>')
    temp_file.close()

    return task_low_level_submission('bibupload', 'remove-duplicate-024', '-P', '3',
                                     '-%s' % mode, temp_path, '--notimechange')


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def create_xml(recid, tags_024):
    record = {}
    record_add_field(record, '001', controlfield_value=str(recid))

    for doi in set(tags_024):
        subfields = [('2', 'DOI'), ('a', doi)]
        record_add_field(record, '024', '7', subfields=subfields)

    return print_rec(record)


def main():
    to_process = []

    max_id = run_sql("SELECT max(id) FROM bibrec")[0][0]
    recids = xrange(1, max_id + 1)

    for done, recid in enumerate(recids):
        tags_024 = get_fieldvalues(recid, '024__a')
        if len(tags_024) > len(set(tags_024)):
            print 'processing %s' % recid
            xml = create_xml(recid, tags_024)
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
