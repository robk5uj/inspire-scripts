import sys

from invenio.search_engine import search_pattern
from invenio.bibdocfile import BibRecDocs


def look_for_fulltext(recid):
    rec_info = BibRecDocs(recid)
    docs = rec_info.list_bibdocs()

    def check_doc(doc):
        for d in doc.list_all_files():
            if d.get_format().strip('.') in ['pdf', 'pdfa', 'PDF']:
                return True
        return False

    return (d for d in docs if check_doc(d))


if __name__ == '__main__':
    verbose = '-v' in sys.argv

    recids = search_pattern(p='arxiv', f='reportnumber')

    for count, recid in enumerate(recids):
        if count % 1000 == 0:
            print 'done %s of %s' % (count, len(recids))

        if verbose:
            print 'processing', recid

        for doc in look_for_fulltext(recid):
            for flag in ['HIDDEN']:
                for ext in ('.pdf', '.PDF'):
                    if doc.has_flag(flag, ext):
                        doc.unset_flag(flag, ext)
