from job_helper import ChunkedBibRank, \
                       loop, \
                       all_recids

from invenio.dbquery import run_sql
from invenio.bibrank_tag_based_indexer import fromDB


SCRIPT_NAME = 'recount_selfcites'


def main():
    bibrank = ChunkedBibRank(methods='selfcites', user=SCRIPT_NAME)
    selfcites = fromDB('selfcites')

    def cb_process_one(recid):
        actual_counts = run_sql('SELECT COUNT(*) FROM rnkSELFCITEDICT WHERE citee = %s', [recid])[0][0]
        if actual_counts and recid not in selfcites:
            bibrank.add(recid)

    loop(all_recids(), cb_process_one)


if __name__ == '__main__':
    main()
