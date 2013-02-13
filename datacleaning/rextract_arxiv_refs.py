import time

from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission
from invenio.search_engine import perform_request_search


def submit_task(to_submit):
    recids = ','.join(str(recid) for recid in to_submit)
    return task_low_level_submission('refextract', 'refextract-arxiv',
                                     '-P', '3',
                                     '-r', recids)


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def main():
    to_process = []

    recids = perform_request_search(p="datecreated:2012-04-01->2013-01-30 and 037__9:arXiv -999c5:curator")

    for done, recid in enumerate(recids):
        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

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
    try:
        main()
    except KeyboardInterrupt:
        print 'Exiting'
