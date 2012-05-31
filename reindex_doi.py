from invenio.bibtask import task_low_level_submission
from invenio.search_engine import perform_request_search
from invenio.dbquery import run_sql
import time


def submit_bibindex_task(to_update):
    recids = [str(r) for r in to_update]
    return task_low_level_submission('bibindex', 'copy-doi', '-w', 'global',
                              '-i', ','.join(recids))


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def main():
    to_update = []

    recids = perform_request_search(p='773:2012 773__a:10*')
    for done, recid in enumerate(recids):

        to_update.append(recid)

        if done % 4000 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        if len(to_update) == 4000 or done + 1 == len(recids) and len(to_update) > 0:
            wait_for_task(submit_bibindex_task(to_update))
            to_update = []


if __name__ == '__main__':
    main()
