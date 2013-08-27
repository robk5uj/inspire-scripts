from invenio.search_engine import perform_request_search
from datetime import datetime, timedelta
from invenio.bibdocfile import BibRecDocs, InvenioBibDocFileError


def list_pdfs(recid):
    rec_info = BibRecDocs(recid)
    docs = rec_info.list_bibdocs()

    for doc in docs:
        for ext in ('pdf', 'pdfa', 'PDF'):
            try:
                yield doc, doc.get_file(ext)
            except InvenioBibDocFileError:
                pass


def main():
    now = datetime.now()
    datecut = now - timedelta(days=365)
    for recid in perform_request_search(p="980__a:arxiv"):
        pdfs = list(list_pdfs(recid))

        max_version = 0
        for _, pdf in pdfs:
            max_version = max(max_version, pdf.get_version())

        for doc, pdf in pdfs:
            if pdf.get_type() == 'arXiv' and pdf.get_version() < max_version and pdf.md < datecut:
                # We can clean this
                doc.delete_file(pdf.get_format(), pdf.get_version())



if __name__ == '__main__':
    main()
