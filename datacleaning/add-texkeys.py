from invenio.search_engine import perform_request_search
from invenio.bibrecord import field_get_subfield_values, \
                              record_get_field_instances, \
                              record_add_field, print_rec
from invenio.dbquery import run_sql
from invenio.bibedit_utils import get_record
from invenio.sequtils_texkey import TexkeySeq, TexkeyNoAuthorError
from invenio.bibtask import task_low_level_submission
from invenio.config import CFG_TMPDIR

import sys
import os
import time
from tempfile import mkstemp

PREFIX = "texkey-upload"


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


def submit_bibindex_task(to_update):
    recids = [str(r) for r in to_update]
    return task_low_level_submission('bibindex', 'add-texkeys', '-w', 'global',
                                     '-i', ','.join(recids))


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def process_chunk(to_process):
    task_id = submit_task(to_process, 'a')
    print 'submitted task id %s' % task_id
    wait_for_task(task_id)


def create_xml(recid, texkey):
    record = {}
    record_add_field(record, '001', controlfield_value=str(recid))
    subfields_toadd = [('a', texkey), ('9', 'INSPIRETeX')]
    record_add_field(record, tag='035', subfields=subfields_toadd)
    return print_rec(record)


def main():
    verbose = '-v' in sys.argv

    recids = perform_request_search(p='-035:spirestex -035:inspiretex', cc='HEP')
    print "Found %s records to assign texkeys" % len(recids)
    processed = []
    to_process = []
    for count, recid in enumerate(recids):
        if count % 300 == 0:
            print 'done %s of %s' % (count, len(recids))

        if verbose:
            print "processing ", recid

        # Check that the record does not have already a texkey
        has_texkey = False
        recstruct = get_record(recid)
        for instance in record_get_field_instances(recstruct, tag="035", ind1="", ind2=""):
            try:
                provenance = field_get_subfield_values(instance, "9")[0]
            except IndexError:
                provenance = ""
            try:
                value = field_get_subfield_values(instance, "z")[0]
            except IndexError:
                value = ""
            provenances = ["SPIRESTeX", "INSPIRETeX"]
            if provenance in provenances and value:
                has_texkey = True
                print "INFO: Record %s has already texkey %s" % (recid, value)

        if not has_texkey:
            TexKeySeq = TexkeySeq()
            new_texkey = ""
            try:
                new_texkey = TexKeySeq.next_value(recid)
            except TexkeyNoAuthorError:
                print "WARNING: Record %s has no first author or collaboration" % recid
                continue
            xml = create_xml(recid, new_texkey)
            processed.append(recid)
            to_process.append(xml)

        if len(to_process) == 500:
            process_chunk(to_process)
            to_process = []

    if to_process:
        process_chunk(to_process)

    # Finally, index all the records processed
    if processed:
        submit_bibindex_task(processed)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print 'Exiting'
