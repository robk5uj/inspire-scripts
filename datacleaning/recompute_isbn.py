from job_helper import ChunkedBibRank, loop
from invenio.search_engine import search_pattern


SCRIPT_NAME = 'bibrank-recid'


def main():
    refextract = ChunkedBibRank(user=SCRIPT_NAME, methods='citation')

    def cb_process_one(recid):
        refextract.add(recid)

    recids = search_pattern(p='999C50:/.+/')
    loop(recids, cb_process_one)

    refextract.cleanup()


if __name__ == '__main__':
    main()
