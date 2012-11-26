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

PREFIX = 'reformat-hb-arxiv'


def submit_task(to_submit):
    recids = ','.join(str(recid) for recid in to_submit)
    return task_low_level_submission('bibreformat', PREFIX, '-i', recids,
                                     '-o', 'HB')


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def extract_arxiv_ids_from_recid(recid):
    for report_number in get_fieldvalues(recid, '037__a'):
        if not report_number.startswith('arXiv'):
            continue

        # Extract arxiv id
        try:
            yield report_number.split(':')[1]
        except IndexError:
            pass


def main():
    to_process = []

    max_id = run_sql("SELECT max(id) FROM bibrec")[0][0]
    recids = xrange(1, max_id + 1)

    for done, recid in enumerate(recids):
        if list(extract_arxiv_ids_from_recid(recid)):
            to_process.append(recid)

        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        if len(to_process) == 1000 or done + 1 == len(recids) and len(to_process) > 0:
            task_id = submit_task(to_process)
            print 'submitted task id %s' % task_id
            wait_for_task(task_id)
            to_process = []


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print 'Exiting'
