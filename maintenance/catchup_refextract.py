import time
import re

from invenio.bibtask import task_low_level_submission
from invenio.dbquery import run_sql


PREFIX = 'ref-catchup'


def submit_bibrank(to_submit):
    return task_low_level_submission('refextract', PREFIX, '-P', '3',
                                     '--no-overwrite',
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

    regex = re.compile("Record ([0-9]+) DONE", re.DOTALL)

    recfile = "/opt/cds-invenio/var/log/bibsched/39/bibsched_task_393308.log"
    recids = []
    with open(recfile) as recs:
        for recid in regex.findall(recs.read()):
            recids.append(recid)

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
