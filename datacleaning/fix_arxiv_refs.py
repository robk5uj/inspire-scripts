import re
from invenio.bibedit_utils import get_record_revision_ids
from invenio.bibeditcli import get_marcxml_of_revision_id, \
                               get_xml_comparison

from tempfile import mkstemp
import os
import time

from invenio.config import CFG_TMPDIR
from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission
from invenio.search_engine import get_record
from invenio.bibrecord import print_rec, \
                              record_get_field_instances, \
                              record_add_field, \
                              record_add_fields, \
                              field_get_subfield_instances


PREFIX = 'arxiv-refs-fix'


# Pattern for old arxiv numbers
old_arxiv_numbers = ur"[\|/-]?(?P<num>(?:9[1-9]|0[0-7])(?:0[1-9]|1[0-2])\d{3})"
old_arxiv = {
    ur"acc-ph": None,
    ur"astro-ph": None,
    ur"astro-phy": "astro-ph",
    ur"astro-ph\.[a-z]{2}": None,
    ur"atom-ph": None,
    ur"chao-dyn": None,
    ur"chem-ph": None,
    ur"cond-mat": None,
    ur"cs": None,
    ur"cs\.[a-z]{2}": None,
    ur"gr-qc": None,
    ur"hep-ex": None,
    ur"hep-lat": None,
    ur"hep-ph": None,
    ur"hepph": "hep-ph",
    ur"hep-th": None,
    ur"hepth": "hep-th",
    ur"math": None,
    ur"math\.[a-z]{2}": None,
    ur"math-ph": None,
    ur"nlin": None,
    ur"nlin\.[a-z]{2}": None,
    ur"nucl-ex": None,
    ur"nucl-th": None,
    ur"physics": None,
    ur"physics\.acc-ph": None,
    ur"physics\.ao-ph": None,
    ur"physics\.atm-clus": None,
    ur"physics\.atom-ph": None,
    ur"physics\.bio-ph": None,
    ur"physics\.chem-ph": None,
    ur"physics\.class-ph": None,
    ur"physics\.comp-ph": None,
    ur"physics\.data-an": None,
    ur"physics\.ed-ph": None,
    ur"physics\.flu-dyn": None,
    ur"physics\.gen-ph": None,
    ur"physics\.geo-ph": None,
    ur"physics\.hist-ph": None,
    ur"physics\.ins-det": None,
    ur"physics\.med-ph": None,
    ur"physics\.optics": None,
    ur"physics\.plasm-ph": None,
    ur"physics\.pop-ph": None,
    ur"physics\.soc-ph": None,
    ur"physics\.space-ph": None,
    ur"plasm-ph": "physics\.plasm-ph",
    ur"q-bio\.[a-z]{2}": None,
    ur"q-fin\.[a-z]{2}": None,
    ur"q-alg": None,
    ur"quant-ph": None,
    ur"quant-phys": "quant-ph",
    ur"solv-int": None,
    ur"stat\.[a-z]{2}": None,
    ur"stat-mech": None,
    ur"dg-ga": None,
    ur"hap-ph": "hep-ph",
    ur"funct-an": None,
    ur"quantph": "quant-ph",
    ur"stro-ph": "astro-ph",
    ur"hepex": "hep-ex",
    ur"math-ag": "math.ag",
    ur"math-dg": "math.dg",
    ur"nuc-th": "nucl-th",
    ur"math-ca": "math.ca",
    ur"nlin-si": "nlin.si",
    ur"quantum-ph": "quant-ph",
    ur"ep-ph": "hep-ph",
    ur"ep-th": "hep-th",
    ur"ep-ex": "hep-ex",
    ur"hept-h": "hep-th",
    ur"hepp-h": "hep-ph",
    ur"physi-cs": "physics",
    ur"asstro-ph": "astro-ph",
    ur"hep-lt": "hep-lat",
    ur"he-ph": "hep-ph",
    ur"het-ph": "hep-ph",
    ur"mat-ph": "math.th",
    ur"math-th": "math.th",
    ur"ucl-th": "nucl-th",
    ur"nnucl-th": "nucl-th",
    ur"nuclt-th": "nucl-th",
    ur"atro-ph": "astro-ph",
    ur"qnant-ph": "quant-ph",
    ur"astr-ph": "astro-ph",
    ur"math-qa": "math.qa",
    ur"tro-ph": "astro-ph",
    ur"hucl-th": "nucl-th",
    ur"math-gt": "math.gt",
    ur"math-nt": "math.nt",
    ur"math-ct": "math.ct",
    ur"math-oa": "math.oa",
    ur"math-sg": "math.sg",
    ur"math-ap": "math.ap",
    ur"quan-ph": "quant-ph",
    ur"nlin-cd": "nlin.cd",
    ur"math-sp": "math.sp",
    ur"atro-ph": "astro-ph",
    ur"ast-ph": "astro-ph",
    ur"asyro-ph": "astro-ph",
    ur"aastro-ph": "astro-ph",
    ur"astrop-ph": "astro-ph",
    ur"arxivastrop-ph": "astro-ph",
    ur"hept-th": "hep-th",
    ur"quan-th": "quant-th",
    ur"asro-ph": "astro-ph",
    ur"castro-ph": "astro-ph",
    ur"asaastro-ph": "astro-ph",
    ur"hhep-ph": "hep-ph",
    ur"hhep-ex": "hep-ex",
    ur"alg-geom": None,
    ur"nuclth": "nucl-th",
}


def compute_arxiv_re(report_pattern, report_number):
    if report_number is None:
        report_number = ur"\g<name>"
    report_re = re.compile("^(?P<name>" + report_pattern + ")" \
                            + old_arxiv_numbers + "$", re.U|re.I)
    return report_re, report_number

RE_OLD_ARXIV = [compute_arxiv_re(*i) for i in old_arxiv.iteritems()]


def tag_arxiv_more(line):
    """Tag old arxiv report numbers"""
    for report_re, report_repl in RE_OLD_ARXIV:
        report_number = report_repl + ur"/\g<num>"
        line = report_re.sub(report_number, line)
    return line


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


def submit_bibindex(to_submit):
    return task_low_level_submission('bibindex', PREFIX, '-P', '3',
                                     '-w', 'reference', '-N', 'ref',
                                     '-i', ','.join(str(i) for i in to_submit))


def submit_bibrank(to_submit):
    return task_low_level_submission('bibrank', PREFIX, '-P', '3',
                                     '-w', 'citation',
                                     '-i', ','.join(str(i) for i in to_submit))


def wait_for_task(task_id):
    sql = 'SELECT status FROM schTASK WHERE id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def create_xml(recid, old_record):
    fields_to_add = record_get_field_instances(old_record, tag='999', ind1='C', ind2='5')

    record = {}
    record_add_field(record, '001', controlfield_value=str(recid))
    record_add_fields(record, '999', fields_to_add)
    return print_rec(record)


def modifiy_record(record):
    fields = record_get_field_instances(record, tag='999', ind1='C', ind2='5')
    for f in fields:
        subfields = field_get_subfield_instances(f)
        for index, s in enumerate(subfields):
            if s[0] == 'r':
                rn = tag_arxiv_more(s[1].decode('utf-8'))
                subfields[index] = ('r', rn.encode('utf-8'))


def process_chunk(to_process, to_process_ids):
    # print to_process[0]
    # return
    # Bibupload
    task_id = submit_task(to_process, 'z')
    print 'submitted task id %s' % task_id
    wait_for_task(task_id)
    for recid in to_process_ids:
        revid2, revid1 = get_record_revision_ids(recid)[:2]
        xml1 = get_marcxml_of_revision_id(revid1)
        xml2 = get_marcxml_of_revision_id(revid2)
        diff = get_xml_comparison(revid1, revid2, xml1, xml2)
        out = open('/tmp/%s-out.xml' % PREFIX, 'a')
        try:
            out.write(diff)
        finally:
            out.close()

    # # Bibindex
    # task_id = submit_bibindex(to_process_ids)
    # print 'submitted task id %s' % task_id
    # wait_for_task(task_id)
    # # Bibrank
    # task_id = submit_bibrank(to_process_ids)
    # print 'submitted task id %s' % task_id
    # wait_for_task(task_id)


def main():
    processed = set()
    to_process = []
    to_process_ids = []

    ids_file = open('/tmp/arxiv-fix.ids', 'a')

    max_id = run_sql("SELECT max(id) FROM bibrec")[0][0]
    recids = xrange(536167, max_id + 1)
    try:

        for done, recid in enumerate(recids):
            record = get_record(recid)
            old_xml = create_xml(recid, record)

            modifiy_record(record)

            new_xml = create_xml(recid, record)
            if old_xml != new_xml:
                print 'adding', recid
                to_process.append(new_xml)
                ids_file.write("%s\n" % recid)
                # to_process_ids.append(recid)
                # processed.add(recid)

            if done % 2000 == 0:
                print 'done %s of %s' % (done + 1, len(recids))

            # if len(to_process) == 500:
            #     process_chunk(to_process, to_process_ids)
            #     to_process = []
            #     to_process_ids = []

        if to_process:
            process_chunk(to_process, to_process_ids)

    finally:
        print 'processed', repr(processed)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print 'Exiting'
