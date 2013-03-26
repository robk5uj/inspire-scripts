from job_helper import ChunkedRefextract, \
                       loop, \
                       all_recids
from invenio.docextract_record import get_record
from invenio.refextract_api import record_has_fulltext

SCRIPT_NAME = 'catchup-refextract'


def main():
    refextract = ChunkedRefextract(user=SCRIPT_NAME)

    def cb_process_one(recid):
        record = get_record(recid)
        if record.find_fields('999C5') or record.find_fields('999C6'):
            return
        if record_has_fulltext(recid):
            refextract.add(recid)

    loop(all_recids(), cb_process_one)


if __name__ == '__main__':
    main()
