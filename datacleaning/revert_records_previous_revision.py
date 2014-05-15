from invenio.bibedit_utils import get_record_revision_ids
from invenio.bibeditcli import get_info_of_revision_id, \
                               get_marcxml_of_revision_id, \
                               save_xml_record


def load_ids():
    f = open('recids.txt')
    for line in f:
        if line.strip():
            yield int(line)


def rollback_record(recid):
        print 'id', recid
        revisions = get_record_revision_ids(recid)
        print 'reverting to ', get_info_of_revision_id(revisions[1])
        xml_record = get_marcxml_of_revision_id(revisions[1])
        save_xml_record(recid, 0, xml_record)


def main():
    # Create dict with
    # recid -> number of revisions to rollback
    for recid in load_ids():
        rollback_record(recid)


if __name__ == '__main__':
    main()
