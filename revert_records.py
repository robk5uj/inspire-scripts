from tempfile import mkstemp
import os

from invenio.bibedit_utils import get_record_revision_ids
from invenio.bibeditcli import get_info_of_revision_id, \
                               get_marcxml_of_revision_id, \
                               save_xml_record
from invenio.config import CFG_TMPSHAREDDIR
from invenio.bibtask import task_low_level_submission


def load_ids():
    f = open('ids2.txt')
    for line in f:
        if line.strip():
            yield int(line)


def rollback_record(recid):
        print 'id', recid
        revisions = get_record_revision_ids(recid)
        rev_count = 0
        rev_info = get_info_of_revision_id(revisions[rev_count])
        while 'arxiv-refs-fix' in rev_info:
            rev_count += 1
            rev_info = get_info_of_revision_id(revisions[rev_count])

        if rev_count == 0:
            print "%s not matching %s" % (recid, rev_info)
        else:
            print 'reverting to ', get_info_of_revision_id(revisions[rev_count])
            xml_record = get_marcxml_of_revision_id(revisions[rev_count])
            save_xml_record(recid, 0, xml_record)


def main():
    # Create dict with
    # recid -> number of revisions to rollback
    for recid in load_ids():
        rollback_record(recid)


if __name__ == '__main__':
    main()
