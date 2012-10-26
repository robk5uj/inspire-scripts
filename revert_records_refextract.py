from tempfile import mkstemp
import os

from invenio.bibedit_utils import get_record_revision_ids
from invenio.bibeditcli import get_info_of_revision_id, \
                               get_marcxml_of_revision_id
from invenio.bibrecord import create_record, \
                              record_get_field_instances, \
                              record_add_fields, \
                              record_add_field, \
                              print_rec
from invenio.config import CFG_TMPSHAREDDIR
from invenio.bibtask import task_low_level_submission


def load_ids():
    f = open('to_process_cut.txt')
    for line in f:
        if line.strip():
            yield int(line)


def create_our_record(recid, refs):
    record = {}
    record_add_field(record, '001', controlfield_value=str(recid))
    record_add_fields(record, '999', refs)
    return print_rec(record)


def rollback_record(recid, weight):
        print 'id', recid, 'weight', weight
        for rev in get_record_revision_ids(recid):
            if weight == 0:
                break
            if 'refextract' in get_info_of_revision_id(rev):
                weight -= 1
        print 'rev', rev
        old_record = create_record(get_marcxml_of_revision_id(rev))
        fields_to_add = record_get_field_instances(old_record[0],
                                                   tag='999',
                                                   ind1='%',
                                                   ind2='%')
        submit_xml(create_our_record(recid, fields_to_add))


def submit_xml(xml):
    # Save new record to file
    (temp_fd, temp_path) = mkstemp(prefix='refextract-fixup',
                                   dir=CFG_TMPSHAREDDIR)
    temp_file = os.fdopen(temp_fd, 'w')
    temp_file.write(xml)
    temp_file.close()

    # Update record
    task_low_level_submission('bibupload', 'refextract-fixup', '-P', '5',
                              '-c', temp_path)


def main():
    # Create dict with
    # recid -> number of revisions to rollback
    recids_weight = {}
    raw_recids = tuple(sorted(load_ids()))
    for recid in raw_recids:
        recids_weight[recid] = 0
    for recid in raw_recids:
        recids_weight[recid] += 1

    for recid, weight in recids_weight.items():
        rollback_record(recid, weight)

if __name__ == '__main__':
    main()
