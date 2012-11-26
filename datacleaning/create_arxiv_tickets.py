from invenio.dbquery import run_sql
from invenio.search_engine_utils import get_fieldvalues
from invenio.bibcatalog_system_rt import BibCatalogSystemRT
from invenio.refextract_task import create_ticket


def main():
    bibcatalog_system = BibCatalogSystemRT()

    max_id = run_sql("SELECT max(id) FROM bibrec")[0][0]
    recids = xrange(1, max_id + 1)

    for done, recid in enumerate(recids):
        if recid < 1124295:
            continue
        if recid >= 1183878:
			break
        if get_fieldvalues(recid, '999C6a') \
                                      and not get_fieldvalues(recid, '999C59'):
            print '* processing', recid
            create_ticket(recid, bibcatalog_system)

        if (done + 1) % 25 == 0:
            print 'done %s of %s' % (done + 1, len(recids))


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print 'Exiting'
