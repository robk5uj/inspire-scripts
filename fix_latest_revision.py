from invenio.intbitset import intbitset
from invenio.dbquery import run_sql

recids = list(intbitset(run_sql("SELECT id FROM bibrec", run_on_slave=True)))
recids.reverse()
for recid in recids:
    try:
        last_updated = run_sql("SELECT last_updated FROM bibfmt WHERE id_bibrec=%s AND format='xm'", (recid, ), run_on_slave=True)[0][0]
        job_date = run_sql("SELECT max(job_date) FROM hstRECORD where id_bibrec=%s GROUP BY id_bibrec", (recid,), run_on_slave=True)[0][0]
    except:
        print "skipping %s" % recid
        continue
    if last_updated > job_date:
        print "Correcting %s" % recid
        run_sql("INSERT INTO hstRECORD(id_bibrec, marcxml, job_id, job_name, job_person, job_date, job_details) VALUES(%s, %s, -1, 'fix_latest_revision', 'kaplun', %s, '')", (recid, run_sql("SELECT value FROM bibfmt WHERE id_bibrec=%s AND format='xm'", (recid, ))[0][0], last_updated))
