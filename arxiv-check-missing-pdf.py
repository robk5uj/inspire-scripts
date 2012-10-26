import time

from invenio.search_engine import search_pattern
from invenio.refextract_api import record_has_fulltext
from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission


def submit_task(to_submit, mode):
    recids = ','.join(str(recid) for recid in to_submit)
    return task_low_level_submission('arxiv-pdf-checker', 'arxiv-pdf-check',
                                     '-P', '3', '-%r', recids)


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def main():
    recids = search_pattern(p='arxiv', f='reportnumber')
    to_process = []

    for count, recid in enumerate(recids):
        if count % 50 == 0:
            print 'done %s of %s' % (count, len(recids))

        if not record_has_fulltext(recid):
            print 'adding', recid
            to_process.append(recid)

        if len(to_process) == 1000:
            task_id = submit_task(to_process)
            print 'submitted task id %s' % task_id
            wait_for_task(task_id)
            to_process = []

    if to_process:
        task_id = submit_task(to_process)
        print 'submitted final task id %s' % task_id


if __name__ == '__main__':
    main()
