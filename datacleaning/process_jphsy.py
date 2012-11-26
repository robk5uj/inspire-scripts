import time

from invenio.bibtask import task_low_level_submission
from invenio.dbquery import run_sql


PATH = '/afs/cern.ch/user/a/adeiana/jphysg.ids'
PREFIX = 'jphys-indexing'


def load_ids(path):
    for line in open(path):
        if line.strip():
            yield int(line)


def submit_bibindex(to_submit):
    return task_low_level_submission('bibindex', PREFIX, '-P', '3',
                                     '-w', 'reference', '-N', 'other',
                                     '-i', ','.join(str(i) for i in to_submit))


def submit_bibrank(to_submit):
    return task_low_level_submission('bibrank', PREFIX, '-P', '3',
                                     '-w', 'citation',
                                     '-i', ','.join(str(i) for i in to_submit))


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def process_chunk(to_process):
    task_id = submit_bibindex(to_process)
    print 'submitted task id %s' % task_id
    wait_for_task(task_id)
    task_id = submit_bibrank(to_process)
    print 'submitted task id %s' % task_id
    wait_for_task(task_id)


def process_one(recid):
    return recid


def main(path):
    to_process = []
    recids = list(load_ids(path))
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
    main(PATH)
