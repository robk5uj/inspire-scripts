from tempfile import mkstemp
import os
import time

from invenio.search_engine import perform_request_search
from invenio.config import CFG_TMPDIR
from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission
from invenio.search_engine_utils import get_fieldvalues
from invenio.search_engine import get_record
from invenio.bibrecord import print_rec, \
                              record_add_field, \
                              record_add_fields, \
                              field_get_subfield_instances, \
                              record_get_field_instances


SCRIPT_NAME = '980-spires-clean'


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

    return task_low_level_submission('bibupload', SCRIPT_NAME, '-P', '5',
                                     '-c', temp_path, '--notimechange')


def submit_bibindex_task(to_update):
    recids = [str(r) for r in to_update]
    return task_low_level_submission('bibindex', SCRIPT_NAME, '-w', 'collection',
                                     '-i', ','.join(recids))


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


class OurInstance(object):
    def __init__(self, field):
        self.field = field

    def __eq__(self, b):
        return hash(self) == hash(b)

    def __hash__(self):
        return hash(tuple(field_get_subfield_instances(self.field)))


def create_our_record(recid):
    old_record = get_record(recid)
    instances = record_get_field_instances(old_record, '980')
    new_instances = [l.field for l in set(OurInstance(i) for i in instances)]

    record = {}
    record_add_field(record, '001', controlfield_value=str(recid))
    record_add_fields(record, '980', new_instances)
    return print_rec(record)


def main():
    to_update = []
    to_update_recids = []

    recids = perform_request_search(p="980:HEP")

    for done, recid in enumerate(recids):
        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        count = get_fieldvalues(recid, '980__a').count('HEP')
        if count > 1:
            print recid, count
            xml = create_our_record(recid)
            to_update.append(xml)
            to_update_recids.append(recid)

        if len(to_update) == 1000 or done + 1 == len(recids) and len(to_update) > 0:
            task_id = submit_task(to_update)
            print 'submitted task id %s' % task_id
            wait_for_task(task_id)
            task_id = submit_bibindex_task(to_update_recids)
            print 'submitted task id %s' % task_id
            wait_for_task(task_id)
            to_update = []
            to_update_recids = []


if __name__ == '__main__':
    main()
