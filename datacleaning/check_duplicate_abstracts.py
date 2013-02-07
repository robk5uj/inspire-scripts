from invenio.search_engine import get_fieldvalues
from invenio.dbquery import run_sql


max_id = run_sql("SELECT max(id) FROM bibrec")[0][0]
recids = xrange(1, max_id + 1)

for recid in recids:
    a = get_fieldvalues(recid, '520__a')
    if len(a) > 1 and a[0] == a[1]:
        print recid
