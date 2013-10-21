from invenio.search_engine import perform_request_search
from datetime import datetime, timedelta
from invenio.bibdocfile import BibRecDocs, InvenioBibDocFileError


def list_pdfs(recid):
    rec_info = BibRecDocs(recid)
    docs = rec_info.list_bibdocs()

    for doc in docs:
        for d in doc.list_all_files():
            if d.get_format() in ('.pdf', '.pdfa', '.PDF'):
                try:
                    yield doc, d
                except InvenioBibDocFileError:
                    pass


def main():
    now = datetime.now()
    datecut = now - timedelta(days=365)
    # perform_request_search(p="980__a:arxiv")
    for done, recid in enumerate([29556]):
        if done % 500 == 0:
            print done
        pdfs = list(list_pdfs(recid))

        max_version = 0
        for _, pdf in pdfs:
            max_version = max(max_version, pdf.get_version())

        for doc, pdf in pdfs:
            if pdf.get_type() == 'arXiv' and pdf.get_version() < max_version and pdf.cd < datecut:
                print 'deleting', pdf.get_format(), pdf.get_version(), 'from', recid
                # We can clean this
                doc.delete_file(pdf.get_format(), pdf.get_version())



if __name__ == '__main__':
    main()
