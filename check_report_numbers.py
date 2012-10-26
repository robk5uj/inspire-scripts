import re

from tempfile import mkstemp
import os
import time

from invenio.config import CFG_TMPDIR
from invenio.dbquery import run_sql
from invenio.bibtask import task_low_level_submission
from invenio.search_engine import get_record
from invenio.dbquery import run_sql
from invenio.bibrecord import print_rec, \
                              record_get_field_instances, \
                              record_add_field, \
                              record_add_fields, \
                              field_get_subfield_instances



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
    ur"mat-ph": "math.ph",
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
}


RE_LIKE_ARXIV = re.compile('\w+(\.\w+)?-\w{1,2}[/-|]?(?:9[1-9]|0[0-7])(?:0[1-9]|1[0-2])\d{3}')

def like_arxiv(tag):
    return RE_LIKE_ARXIV.match(tag) is not None


def compute_arxiv_re(report_pattern):
    return re.compile("^(?P<name>" + report_pattern + ")" \
                                        + old_arxiv_numbers + "$", re.U|re.I)

RE_OLD_ARXIV = [compute_arxiv_re(i) for i in old_arxiv.iterkeys()]


def tag_arxiv_more(line):
    """Tag old arxiv report numbers"""
    for report_re in RE_OLD_ARXIV:
        if report_re.match(line):
            return True
    return False


class InvalidReportNumber(Exception):
    pass


def main():
    processed = set()
    to_process = []
    to_process_ids = []

    tags = run_sql("select id, tag, value from bib99x where tag = '999C5r'")

    for done, row in enumerate(tags):
        tag = row[2]
        if like_arxiv(tag) and not tag_arxiv_more(tag):
            if not tag.startswith('ISBN') and not tag.startswith('CONF'):
                print tag

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print 'Exiting'
