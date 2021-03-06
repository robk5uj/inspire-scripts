import time

from invenio.search_engine import (search_pattern,
                                   perform_request_search)
from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission
from invenio.intbitset import intbitset


def submit_task(to_submit):
    recids = ','.join(str(recid) for recid in to_submit)
    return task_low_level_submission('arxiv-pdf-checker', 'arxiv-pdf-checker',
                                     '-P', '3', '-i', recids)


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def main():
    recids = search_pattern(p='arxiv', f='reportnumber')
    recids &= intbitset(perform_request_search(p='find da today-1'))
    to_process = []

    for count, recid in enumerate(recids):
        if count % 50 == 0:
            print 'done %s of %s' % (count, len(recids))

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
