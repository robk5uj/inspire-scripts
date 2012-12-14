from tempfile import mkstemp
import os
import time

from invenio.config import CFG_TMPDIR
from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission
from invenio.search_engine_utils import get_fieldvalues
from invenio.search_engine import perform_request_search, \
                                  get_record
from invenio.bibrecord import print_rec, \
                              record_add_field, \
                              record_get_field_instances, \
                              record_delete_subfield, \
                              record_delete_field


def submit_task(to_submit, mode):
    # Save new record to file
    (temp_fd, temp_path) = mkstemp(prefix='copy-doi',
                                   dir=CFG_TMPDIR)
    temp_file = os.fdopen(temp_fd, 'w')
    temp_file.write('<?xml version="1.0" encoding="UTF-8"?>')
    temp_file.write('<collection>')
    for el in to_submit:
        temp_file.write(el)
    temp_file.write('</collection>')
    temp_file.close()

    return task_low_level_submission('bibupload', 'copy-doi', '-P', '3',
                                     '-%s' % mode, temp_path, '--notimechange')


def wait_for_task(task_id):
    sql = 'select status from schTASK where id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def submit_bibindex_task(to_update):
    recids = [str(r) for r in to_update]
    return task_low_level_submission('bibindex', 'copy-doi', '-w', 'global',
                                     '-P', '3', '-i', ','.join(recids))


def create_xml(recid, dois):
    record = get_record(recid)

    # Delete old 773s
    record_delete_subfield(record, '773', 'a')
    tags_773 = record_get_field_instances(record, tag='773')
    for tag in tags_773:
        if not tag[0]:  # subfields
            record_delete_field(record, '773', field_position_global=tag[4])

    # Add new 024
    for doi in dois:
        subfields = [('2', 'DOI'), ('a', doi)]
        record_add_field(record, '024', '7', subfields=subfields)

    return print_rec(record)


def main():
    to_replace = []
    to_update_recids = []

    recids = perform_request_search(p='773__a:10*')[:6]
    for done, recid in enumerate(recids):

        fields = set(get_fieldvalues(recid, '773__a'))
        if not fields:
            continue

        existing_fields = set(get_fieldvalues(recid, '024__a'))

        fields -= existing_fields

        replace_xml = create_xml(recid, fields)
        to_replace.append(replace_xml)
        to_update_recids.append(recid)

        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        if len(to_replace) == 1000 or done + 1 == len(recids) and len(to_replace) > 0:
            task_id = submit_task(to_replace, 'r')
            print 'submitted task id %s' % task_id
            wait_for_task(task_id)
            task_id = submit_bibindex_task(to_update_recids)
            wait_for_task(task_id)
            to_replace = []
            to_update_recids = []


if __name__ == '__main__':
    main()