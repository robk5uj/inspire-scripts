import sys
import time

from invenio.search_engine import search_pattern
from invenio.search_engine_utils import get_fieldvalues
from invenio.refextract_api import record_has_fulltext
from invenio.downloadutils import download_file, InvenioDownloadError
from invenio.bibdocfile import BibRecDocs

ARXIV_URL_PATTERN = "http://export.arxiv.org/pdf/%s.pdf"


class InvalidReportNumber(Exception):
    pass


def build_arxiv_url(arxiv_id):
    return ARXIV_URL_PATTERN % arxiv_id


def extract_arxiv_ids_from_recid(recid):
    for report_number in get_fieldvalues(recid, '037__a'):
        if not report_number.startswith('arXiv'):
            continue

        # Extract arxiv id
        try:
            yield report_number.split(':')[1]
        except IndexError:
            raise InvalidReportNumber(report_number)


if __name__ == '__main__':
    verbose = '-v' in sys.argv

    recids = search_pattern(p='arxiv', f='reportnumber')

    for count, recid in enumerate(recids):
        if count % 1000 == 0:
            print 'done %s of %s' % (count, len(recids))
        # if recid < 1107928:
        #     continue

        if verbose:
            print 'processing', recid

        if not record_has_fulltext(recid):
            print 'harvesting', recid
            for arxiv_id in extract_arxiv_ids_from_recid(recid):
                url_for_pdf = build_arxiv_url(arxiv_id)
                dest = '/tmp/arxiv-script/%s.pdf' % arxiv_id
                try:
                    path = download_file(url_for_pdf, dest, content_type='pdf')
                except InvenioDownloadError, e:
                    print "failed to download: %s" % e
                else:
                    docs = BibRecDocs(recid)
                    docs.add_new_file(path,
                                      doctype="arXiv",
                                      docname="arXiv:%s" % arxiv_id)
                time.sleep(20)
