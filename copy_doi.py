from tempfile import mkstemp
import os

from invenio.config import CFG_TMPDIR
from invenio.bibtask import task_low_level_submission
from invenio.search_engine_utils import get_fieldvalues
from invenio.search_engine import perform_request_search
from invenio.bibrecord import print_rec, \
                              record_add_field


def submit_task(to_update):
    # Save new record to file
    (temp_fd, temp_path) = mkstemp(prefix='copy-doi',
                                   dir=CFG_TMPDIR)
    temp_file = os.fdopen(temp_fd, 'w')
    temp_file.write('<?xml version="1.0" encoding="UTF-8"?>')
    temp_file.write('<collection>')
    for el in to_update:
        temp_file.write(el)
    temp_file.write('</collection>')
    temp_file.close()

    task_low_level_submission('bibupload', 'copy-doi', '-P', '5',
                              '-c', temp_path, '--notimechange')


def submit_bibindex_task(to_update):
    recids = [str(r) for r in to_update]
    task_low_level_submission('bibindex', 'copy-doi', '-w', 'doi',
                              '-i', ','.join(recids))

def create_our_record(recid, dois):
    record = {}
    record_add_field(record, '001', controlfield_value=str(recid))
    for doi in dois:
        subfields = [('2', 'DOI'), ('a', doi)]
        record_add_field(record, '024', '7', subfields=subfields)
    return print_rec(record)


def main():
    to_update = []
    to_update_recids = []

    recids = perform_request_search(p='773:2010 773__a:10*')
    for done, recid in enumerate(recids):

        fields = set(get_fieldvalues(recid, '773__a'))
        if not fields:
            continue

        existing_fields = set(get_fieldvalues(recid, '024__a'))

        fields -= existing_fields

        if fields:
            xml = create_our_record(recid, fields)
            to_update.append(xml)
            to_update_recids.append(recid)

        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        if len(to_update) == 25:
            submit_task(to_update)
            submit_bibindex_task(to_update_recids)
            to_update = []
            to_update_recids = []


if __name__ == '__main__':
    main()
