#!/usr/bin/env python

"""
Restore the creation_date of a record from what is available in 961__x or from
the earliest history.
"""
from time import ctime

from invenio.dbquery import run_sql
from invenio.intbitset import intbitset
from invenio.search_engine import search_pattern
from invenio.search_engine_utils import get_fieldvalues
from invenio.dateutils import strftime, strptime, get_time_estimator

def has_ingestion_date():
    columns = run_sql("DESC bibrec")
    for column in columns:
        if column[0] == 'ingestion_date':
            return True
    return False
CFG_HAS_INGESTION_DATE = has_ingestion_date()

def get_all_recids():
    all_recids = intbitset(run_sql("SELECT id FROM bibrec"))
    return all_recids - search_pattern(p="DELETED", f="collection")

def get_creation_date_from_961(recid):
    creation_date = get_fieldvalues(recid, "961__x")
    if creation_date:
        creation_date = creation_date[0]
        for date_format in ("%Y-%m-%d", "%Y-%m", "%Y"):
            try:
                return strptime(creation_date, date_format)
            except ValueError:
                pass
    return None

def get_creation_date_from_hst(recid):
    return run_sql("SELECT MIN(job_date) FROM hstRECORD WHERE id_bibrec=%s", (recid, ))[0][0]

def fix_record(recid):
    creation_date = get_creation_date_from_961(recid) or get_creation_date_from_hst(recid)
    if CFG_HAS_INGESTION_DATE:
        run_sql("UPDATE bibrec SET ingestion_date=%s WHERE id=%s", (creation_date, recid))
    else:
        run_sql("UPDATE bibrec SET creation_date=%s WHERE id=%s", (creation_date, recid))

def main():
    all_recids = get_all_recids()
    len_all_recids = len(all_recids)
    time_estimator = get_time_estimator(len_all_recids)
    for i, recid in enumerate(all_recids):
        fix_record(recid)
        expected_time = ctime(time_estimator()[1])
        if i % 1000 == 0:
            print "%s%%" % ((i + 1) * 100 / len_all_recids), "%s out of %s" % (i + 1, len_all_recids), "recid %s" % recid, expected_time

if __name__ == "__main__":
    main()
