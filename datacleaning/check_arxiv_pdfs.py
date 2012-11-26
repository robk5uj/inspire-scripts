from tempfile import mkstemp
import os
import time
import re

from invenio.config import CFG_TMPDIR
from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission
from invenio.search_engine import get_record
from invenio.search_engine_utils import get_fieldvalues
from invenio.bibdocfile import BibRecDocs, InvenioWebSubmitFileError
from invenio.bibrecord import print_rec, \
                              record_get_field_instances, \
                              record_add_field, \
                              record_add_fields, \
                              field_get_subfield_values

PREFIX = 'nucl-phys-proc-suppl-b'

ARXIV_PATTERN = "arXiv:([0-9a-zA-Z-/.]+)\.pdf"
ARXIV_RE = re.compile(ARXIV_PATTERN)


class InvalidReportNumber(Exception):
    pass


def submit_task(to_submit, mode):
    # Save new record to file
    (temp_fd, temp_path) = mkstemp(prefix=PREFIX,
                                   dir=CFG_TMPDIR)
    temp_file = os.fdopen(temp_fd, 'w')
    temp_file.write('<?xml version="1.0" encoding="UTF-8"?>')
    temp_file.write('<collection>')
    for el in to_submit:
        temp_file.write(el)
    temp_file.write('</collection>')
    temp_file.close()

    return task_low_level_submission('bibupload', PREFIX, '-P', '3',
                                     '-%s' % mode, temp_path, '--notimechange')


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def extract_arxiv_ids_from_recid(recid):
    for report_number in get_fieldvalues(recid, '037__a'):
        if not report_number.startswith('arXiv'):
            continue

        # Extract arxiv id
        try:
            yield report_number.split(':')[1]
        except IndexError:
            raise InvalidReportNumber(report_number)


def extract_arxiv_id_from_url(url):
    """Extract arxiv id from url

    e.g. http://inspirehep.net/record/1126984/files/arXiv:1208.1769.pdf

    Raises IndexError on invalid url
    """
    matches = ARXIV_RE.search(url)
    if matches:
        return matches.group(1)


def check_arxiv_url(field, valid_arxiv_ids):
    url = field_get_subfield_values(field, 'u')
    if not url:
        return True
    url = url[0]
    # print 'url', url
    arxiv_id = extract_arxiv_id_from_url(url)
    # print 'id', arxiv_id
    if arxiv_id is None:
        return True
    else:
        return arxiv_id in valid_arxiv_ids


def look_for_fulltext(recid):
    rec_info = BibRecDocs(recid)
    docs = rec_info.list_bibdocs()

    for doc in docs:
        for d in doc.list_all_files():
            if d.get_format().strip('.') in ['pdf', 'pdfa', 'PDF']:
                try:
                    yield doc, d
                except InvenioWebSubmitFileError:
                    pass


def create_xml(recid, arxiv_ids):
    old_record = get_record(recid)
    attached_files = record_get_field_instances(old_record, tag='856', ind1='4')
    fields_to_add = [f for f in attached_files if check_arxiv_url(f, arxiv_ids)]
    record = {}
    record_add_field(record, '001', controlfield_value=str(recid))
    record_add_fields(record, '856', fields_to_add)
    return print_rec(record)


def process_chunk(to_process):
    task_id = submit_task(to_process, 'c')
    print 'submitted task id %s' % task_id
    wait_for_task(task_id)


def main():
    processed = set()
    to_process = []

    max_id = run_sql("SELECT max(id) FROM bibrec")[0][0]
    recids = xrange(1, max_id + 1)
    # recids = xrange(787870, max_id + 1)
    for done, recid in enumerate(recids):
        # print '* processing', recid
        arxiv_ids = list(extract_arxiv_ids_from_recid(recid))
        # print '  * arxiv ids', repr(arxiv_ids)
        if arxiv_ids:
            # Clean marc from erroneous 8564
            old_record = get_record(recid)
            attached_files = record_get_field_instances(old_record, tag='856', ind1='4')
            fields_to_add = [f for f in attached_files if check_arxiv_url(f, arxiv_ids)]
            if fields_to_add != attached_files:
                print '* processing', recid
                print '  * generating xml'
                xml = create_xml(recid, arxiv_ids)
                to_process.append(xml)
                processed.add(recid)
            # Clean bibdocfiles
            for doc, docfile in look_for_fulltext(recid):
                prefix = 'arXiv:'
                if not doc.docname.startswith(prefix):
                    continue
                if doc.docname[len(prefix):] not in arxiv_ids \
                    and doc.docname[len(prefix):] not in (i.replace('/', '-') for i in arxiv_ids) \
                    and doc.docname[len(prefix):] not in (i.replace('/', '_') for i in arxiv_ids):
                    processed.add(recid)
                    print '* processing', recid
                    print '  * arxiv ids', repr(arxiv_ids)
                    print '  * docname', doc.docname, "=>", doc.docname[len(prefix):]
                    print '  * deleting doc file'
                    doc.delete_file(docfile.get_format(), docfile.get_version())
                    if not doc.list_all_files():
                        doc.expunge()

        if done % 2000 == 0:
            print 'done %s of %s' % (done + 1, len(recids))

        if len(to_process) == 1000:
            process_chunk(to_process)
            to_process = []

    if to_process:
        process_chunk(to_process)

    print repr(to_process)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print 'Exiting'
