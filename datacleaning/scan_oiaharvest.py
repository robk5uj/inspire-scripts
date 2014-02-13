import marshal
import getopt
import re
import time

from invenio.dbquery import run_sql
from invenio.search_engine import search_unit
from invenio.intbitset import intbitset
from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission
from invenio.search_engine import perform_request_search
from invenio.bibedit_utils import get_record_revision_ids
from invenio.bibeditcli import get_marcxml_of_revision_id
from invenio.docextract_record import create_record

ARXIV_NUM_RE = re.compile(ur'\d{4}\.\d{4}$')


def get_identifiers(argv):
    """Try to get the runtime by analysing the arguments."""
    identifiers = ""
    argv = list(argv)
    while True:
        try:
            opts, dummy_args = getopt.gnu_getopt(argv, 'i:', ['identifier='])
        except getopt.GetoptError, err:
            ## We remove one by one all the non recognized parameters
            if len(err.opt) > 1:
                argv = [arg for arg in argv if arg != '--%s' % err.opt and not arg.startswith('--%s=' % err.opt)]
            else:
                argv = [arg for arg in argv if not arg.startswith('-%s' % err.opt)]
        else:
            break
    for opt in opts:
        if opt[0] in ('-i', '--identifier'):
            try:
                identifiers = opt[1]
            except ValueError:
                pass
    return identifiers


def main():
    recids = intbitset()
    rows = run_sql('SELECT arguments FROM schTASK where proc = "oaiharvest"')
    for row in rows:
        raw_arguments = row[0]
        identifiers = get_identifiers(marshal.loads(raw_arguments))
        for identifier in identifiers.split(','):
            if identifier:
                identifier = identifier.replace('oai:arXiv.org:', '')
                identifier = identifier.replace('arXiv:', '')
                if ARXIV_NUM_RE.match(identifier):
                    identifier = "arXiv:%s" % identifier
                records = search_unit(p=identifier, f='reportnumber')
                if records:
                    # print "%s => %s" % (identifier, records[0])
                    recids += records

    print 'Fetching possible recids'
    # recids &= perform_request_search(p="datemodified:2013-01-01->2014-01-13 and 037__9:arXiv -999c5:curator", of='intbitset')
    recids &= perform_request_search(p="datemodified:2013-01-01->2014-01-20 and 037__9:arXiv -999c5:curator", of='intbitset')
    print 'Recids to process', len(recids)
    check_recids(recids)


def check_recids(recids):
    to_verify = set()
    for recid in recids:
        print 'id', recid
        for rev in get_record_revision_ids(recid):
            old_record = create_record(get_marcxml_of_revision_id(rev))
            fields = old_record['999C5']
            if fields:
                for f in fields:
                    if f['9']:
                        to_verify.add(recid)
    print repr(to_verify)

def submit_task(to_submit):
    recids = ','.join(str(recid) for recid in to_submit)
    return task_low_level_submission('refextract', 'refextract-arxiv', '-i', recids)


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def process_recids(recids):
    to_process = []

    for done, recid in enumerate(recids):
        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        to_process.append(recid)

        if len(to_process) == 500:
            task_id = submit_task(to_process)
            print 'submitted task id %s' % task_id
            wait_for_task(task_id)
            to_process = []

    if to_process:
        task_id = submit_task(to_process)
        print 'submitted final task id %s' % task_id


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print 'Exiting'
