from datetime import datetime, timedelta

from job_helper import ChunkedBibRank, \
                       loop, \
                       all_recids

from invenio.dbquery import run_sql
from invenio.bibrank_tag_based_indexer import fromDB
from invenio.search_engine_utils import get_fieldvalues
from invenio.bibcatalog_system_rt import BibCatalogSystemRT
from invenio.arxiv_pdf_checker import extract_arxiv_ids_from_recid
from invenio.search_engine import get_record
from invenio.bibfilter_oaiarXiv2inspire import generate_ticket, \
                                               get_minimal_arxiv_id

SCRIPT_NAME = 'missing-tickets'
QUEUE = 'HEP_curation'

def create_ticket(recid, bibcatalog_system):
    record = get_record(recid)
    if not get_minimal_arxiv_id(record):
        return
    subject, text = generate_ticket(record)
    ticket_id = bibcatalog_system.ticket_submit(subject=subject,
                                                queue=QUEUE,
                                                recordid=recid)
    bibcatalog_system.ticket_comment(None,
                                     ticket_id,
                                     text)


def main():
    recids = set()
    now = datetime.now()
    bibcatalog_system = BibCatalogSystemRT()

    def cb_process_one(recid):
        collections = get_fieldvalues(recid, "980__a")
        if 'CORE' not in collections:
            return
        if 'arXiv' not in collections:
            return

        for category in get_fieldvalues(recid, '037__c'):
            if category.startswith('astro-ph'):
                # We do not curate astro-ph
                return

        results = bibcatalog_system.ticket_search(None,
                                                  recordid=recid,
                                                  queue=QUEUE)
        if results:
            return

        print 'missing ticket for #%s' % recid
        recids.add(recid)

        create_ticket(recid, bibcatalog_system)


    loop(all_recids(start=1232152), cb_process_one)

    print recids
    print len(recids)


if __name__ == '__main__':
    main()
