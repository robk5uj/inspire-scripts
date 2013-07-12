# -*- coding: utf-8 -*-

from job_helper import loop, \
                       all_recids
from invenio.dbquery import run_sql
from invenio.docextract_record import get_record

def cb_one(recid):
    record = get_record(recid)
    if 'ß' in record.to_xml():
        print '->', recid
        sql = 'UPDATE bibrec SET modification_date = NOW() WHERE id = %s'
        run_sql(sql, [recid])


if __name__ == '__main__':
    loop(all_recids(), cb_one)
