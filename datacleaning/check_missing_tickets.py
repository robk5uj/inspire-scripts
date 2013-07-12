from datetime import datetime, timedelta

from job_helper import ChunkedBibRank, \
                       loop, \
                       all_recids

from invenio.dbquery import run_sql
from invenio.bibrank_tag_based_indexer import fromDB
from invenio.search_engine_utils import get_fieldvalues
from invenio.bibcatalog_system_rt import BibCatalogSystemRT

SCRIPT_NAME = 'missing-tickets'


def main():
    recids = set()
    now = datetime.now()
    bibcatalog_system = BibCatalogSystemRT()

    def cb_process_one(recid):
        # Do not create tickets for old records
        creation_date = run_sql("""SELECT creation_date FROM bibrec
                                   WHERE id = %s""", [recid])[0][0]
        if creation_date < now - timedelta(days=365*2):
            return

        in_core = False
        in_arxiv = False
        for collection in get_fieldvalues(recid, "980__a"):
            if collection == 'CORE':
                in_core = True
            elif collection == 'arXiv':
                in_arxiv = True

        # Only create tickets for HEP
        if not in_core or not in_arxiv:
            return

        for category in get_fieldvalues(recid, '037__c'):
            if category.startswith('astro-ph'):
                # We do not curate astro-ph
                return

        if not get_fieldvalues(recid, '999C6v'):
            return

        results = bibcatalog_system.ticket_search(None,
                                                  recordid=recid,
                                                  queue='Inspire-References')
        if results:
            return

        results = bibcatalog_system.ticket_search(None,
                                                  recordid=recid,
                                                  queue='HEP_curation')
        if results:
            return

        print 'missing ticket for #%s' % recid
        recids.add(recid)

    # 1118858 is the first refextract ticket
    loop(all_recids(start=1119023), cb_process_one)

    print recids
    print len(recids)


if __name__ == '__main__':
    main()
