import os
import sys
import subprocess
import time
from urllib import urlretrieve
from shutil import copyfile

from invenio.search_engine import search_pattern
from invenio.search_engine_utils import get_fieldvalues
from invenio.bibdocfile import BibRecDocs, InvenioWebSubmitFileError

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


def look_for_fulltext(recid):
    rec_info = BibRecDocs(recid)
    docs = rec_info.list_bibdocs()

    path = None
    bibdoc = None
    for doc in docs:
        for extension in ['pdf', 'pdfa', 'PDF']:
            try:
                path = doc.get_file(extension).get_full_path()
                bibdoc = doc
                break
            except InvenioWebSubmitFileError:
                pass

    return bibdoc, path


def shellquote(s):
    return "'" + s.replace("'", "'\\''") + "'"


def check_pdf(path):
    p = subprocess.Popen(["file", path],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    ret = p.stdout.read().lower().rsplit(':', 1)[-1]
    # print repr(ret)
    return 'pdf' in ret

if __name__ == '__main__':
    verbose = '-v' in sys.argv

    recids = search_pattern(p='arxiv', f='reportnumber')
    # recids = [877748]
    # recids = [1110769]
    # recids = [1107930]

    to_fix = set()

    for count, recid in enumerate(recids):
        if count % 1000 == 0:
            print 'done %s of %s' % (count, len(recids))

        if verbose:
            print 'processing', recid

        doc, path = look_for_fulltext(recid)
        # print repr(path)
        if path and not check_pdf(path):
            print 'invalid pdf for', recid
            print 'path', path
            doc.expunge()
            continue

            print 'fetching pdf for', recid

            for arxiv_id in extract_arxiv_ids_from_recid(recid):
                url = build_arxiv_url(arxiv_id)
                print 'downloading', url
                try:
                    filename, dummy = urlretrieve(url)
                except IOError, e:
                    print 'exception', repr(e)
                    continue
                try:
                    if check_pdf(filename):
                        print 'copying', filename, 'to', path
                        copyfile(filename, path)
                        break
                    else:
                        print 'invalid pdf for', recid
                        print 'path', path
                        doc.expunge()

                        to_fix.add(recid)
                finally:
                    os.unlink(filename)
            time.sleep(10)

print 'to_fix', repr(to_fix)
