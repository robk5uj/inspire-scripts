from invenio.bibedit_utils import get_record_revision_ids
from invenio.bibeditcli import get_info_of_revision_id, \
                               get_marcxml_of_revision_id
from invenio.bibrecord import create_record, \
                              record_get_field_instances, \
                              field_get_subfield_instances
from fix_arxiv_refs import tag_arxiv_more
def load_ids():
    f = open('ids2.txt')
    for line in f:
        if line.strip():
            yield int(line)

def look_for_revisions(recid):
    revisions = get_record_revision_ids(recid)
    rev_count = 0
    rev_info = get_info_of_revision_id(revisions[rev_count])
    bibedit_str = 'bibedit_record_%s_0.xml' % recid
    while 'arxiv-refs-fix' in rev_info or bibedit_str in rev_info:
        rev_count += 1
        rev_info = get_info_of_revision_id(revisions[rev_count])
    return revisions[0], revisions[rev_count]

def get_rn(revision):
    rns = set()
    record = create_record(get_marcxml_of_revision_id(revision))[0]
    fields = record_get_field_instances(record, tag='999', ind1='C', ind2='5')
    for f in fields:
        subfields = field_get_subfield_instances(f)
        for index, s in enumerate(subfields):
            if s[0] == 'r':
                rns.add(tag_arxiv_more(s[1]))
    return rns


def main():
    # Create dict with
    # recid -> number of revisions to rollback
    for recid in load_ids():

        latest_revision, previous_revision = look_for_revisions(recid)
        if get_rn(latest_revision) ^ get_rn(previous_revision):
            print 'recid', recid
            print 'in new'
            for rn in get_rn(latest_revision) - get_rn(previous_revision):
                print '  * ', repr(rn)
            print 'in old'
            for rn in get_rn(previous_revision) - get_rn(latest_revision):
                print '  * ', repr(rn)


if __name__ == '__main__':
    main()
