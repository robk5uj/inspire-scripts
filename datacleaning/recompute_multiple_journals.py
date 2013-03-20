from job_helper import ChunkedBibRank, \
                       loop, \
                       all_recids
from invenio.docextract_record import get_record
from invenio.dbquery import run_sql

SCRIPT_NAME = 'bibrank-multiple-journals'


def main():
    bibrank = ChunkedBibRank(indexes='citation', user=SCRIPT_NAME)

    counter = [0]

    def cb_process_one(recid):
        print 'processing', recid
        try:
            rec = get_record(recid)
        except UnicodeDecodeError:
            pass
        else:
            if len(rec.find_fields('773__')) > 1:
                counter[0] += 1
                # bibrank.add(recid)


    max_id = run_sql("SELECT max(id) FROM bibrec")[0][0]
    loop(xrange(1, max_id + 1), cb_process_one)
    print 'total', counter[0]

if __name__ == '__main__':
    main()
