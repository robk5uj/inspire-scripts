import time

from invenio.bibtask import task_low_level_submission
from invenio.dbquery import run_sql


PREFIX = 'bibrank-catchup'


def submit_bibrank(to_submit):
    return task_low_level_submission('bibrank', PREFIX, '-P', '3',
                                     '-w', 'citation',
                                     '-i', ','.join(str(i) for i in to_submit))


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def process_chunk(to_process):
    task_id = submit_bibrank(to_process)
    print 'submitted task id %s' % task_id
    wait_for_task(task_id)


def process_one(recid):
    return recid


def main():
    to_process = []

    query = """SELECT id FROM bibrec
               WHERE modification_date >= '2012-09-22 21:13:43'
               AND modification_date <= '2012-09-26 00:00:00'"""
    recids = [r[0] for r in run_sql(query)]
    for done, recid in enumerate(recids):
        if done % 250 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        to_process.append(process_one(recid))

        if len(to_process) == 1000:
            process_chunk(to_process)
            to_process = []

    if to_process:
        process_chunk(to_process)


if __name__ == '__main__':
    main()
