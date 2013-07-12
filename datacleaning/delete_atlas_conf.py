from tempfile import mkstemp
import os
import time

from invenio.search_engine import perform_request_search
from invenio.config import CFG_TMPDIR
from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission
from invenio.search_engine_utils import get_fieldvalues
from invenio.bibrecord import print_rec, \
                              record_add_field


SCRIPT_NAME = 'delete-atlas-conf'


def submit_task(to_update):
    # Save new record to file
    (temp_fd, temp_path) = mkstemp(prefix=SCRIPT_NAME,
                                   dir=CFG_TMPDIR)
    temp_file = os.fdopen(temp_fd, 'w')
    temp_file.write('<?xml version="1.0" encoding="UTF-8"?>')
    temp_file.write('<collection>')
    for el in to_update:
        temp_file.write(el)
    temp_file.write('</collection>')
    temp_file.close()

    return task_low_level_submission('bibupload', SCRIPT_NAME, '-a', temp_path)



def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def create_our_record(recid):
    record = {}
    record_add_field(record, '001', controlfield_value=str(recid))
    subfields = [('a', 'DELETED')]
    record_add_field(record, '980', subfields=subfields)
    return print_rec(record)


def main():
    to_update = []

    recids = perform_request_search(p="reportnumber:ATLAS-CONF-*")
    for done, recid in enumerate(recids):

        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        xml = create_our_record(recid)
        to_update.append(xml)

        if len(to_update) == 1000 or done + 1 == len(recids) and len(to_update) > 0:
            task_id = submit_task(to_update)
            print 'submitted task id %s' % task_id
            wait_for_task(task_id)
            to_update = []


if __name__ == '__main__':
    main()
