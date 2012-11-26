from tempfile import mkstemp
import os
import time
import re

from invenio.config import CFG_TMPDIR
from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission
from invenio.search_engine import get_record
from invenio.search_engine_utils import get_fieldvalues
from invenio.bibdocfile import BibRecDocs, InvenioWebSubmitFileError
from invenio.bibrecord import print_rec, \
                              record_get_field_instances, \
                              record_add_field, \
                              record_add_fields, \
                              field_get_subfield_values

PREFIX = 'nucl-phys-proc-suppl-b'


def submit_task(to_submit):
    return task_low_level_submission('refextract', PREFIX, '-P', '3',
                                     '--recids',
                                     ','.join(str(r) for r in to_submit))


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def process_chunk(to_process):
    task_id = submit_task(to_process)
    print 'submitted task id %s' % task_id
    wait_for_task(task_id)


def load_ids(path):
    for line in open(path):
        if line.strip():
            yield int(line)


def main():
    to_process = []

    recids = list(load_ids('to_process.txt'))
    for done, recid in enumerate(recids):
        if get_fieldvalues(recid, '999C6a') \
                                      and not get_fieldvalues(recid, '999C59'):
            print '* processing', recid
            to_process.append(recid)

        if (done + 1) % 25 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

    if to_process:
        process_chunk(to_process)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print 'Exiting'
