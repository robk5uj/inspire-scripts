import time

from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission


def submit_task(to_submit):
    recids = ','.join(str(recid) for recid in to_submit)
    return task_low_level_submission('refextract', '', '-P', '3',
                                     '-r', recids)


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def main():
    to_process = []

    max_id = run_sql("SELECT max(id) FROM bibrec")[0][0]
    recids = xrange(1, max_id + 1)

    for done, recid in enumerate(recids):
        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

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
