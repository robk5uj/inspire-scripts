import time

from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission
from invenio.bibrank_citation_indexer import get_bibrankmethod_lastupdate

FORMAT = 'HB'


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def submit(recids):
    print 'submitting %s' % str(recids)
    task_id = task_low_level_submission('bibreformat', 'catchup-doi', '-o', FORMAT, '-P', '5', '-i', ','.join(str(r) for r in recids))
    wait_for_task(task_id)


max_id = run_sql("SELECT max(id) FROM bibrec")[0][0]
latest_bibrank_run = get_bibrankmethod_lastupdate('citation')


recids = xrange(1, max_id + 1)
to_update = []

for recid in recids:
    if recid % 50 == 0:
        print '%s of %s' % (recid, max_id)

    ret = run_sql('SELECT id FROM bibrec WHERE id = %s', [recid])
    if not ret:
        continue

    ret = run_sql('SELECT id_bibrec FROM bibfmt WHERE format = %s AND id_bibrec = %s', [FORMAT, recid])
    if not ret:
        to_update.append(recid)

        if len(to_update) == 1000:
            submit(to_update)
            to_update = []

print 'almost done'
if to_update:
    submit(to_update)
print 'done'
# print 'to_update', repr(to_update)
