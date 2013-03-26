from job_helper import ChunkedBibRank, \
                       loop, \
                       all_recids
from invenio.docextract_record import get_record

SCRIPT_NAME = 'bibrank-multiple-journals'


def main():
    bibrank = ChunkedBibRank(methods='citation', user=SCRIPT_NAME)

    def cb_process_one(recid):
        print 'processing', recid
        try:
            rec = get_record(recid)
        except UnicodeDecodeError:
            pass
        else:
            if len(rec.find_fields('773__')) > 1:
                bibrank.add(recid)


    loop(all_recids(), cb_process_one)

if __name__ == '__main__':
    main()
