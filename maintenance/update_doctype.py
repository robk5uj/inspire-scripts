atypes = ('Main', '') # list of initial doctypes to be changed from
btype = 'INSPIRE-PUBLIC' # final doctype value to be changed into

import os
from invenio.bibdocfile import BibDoc
from invenio.dbquery import run_sql

for atype in atypes:
    res = run_sql("SELECT id_bibrec,id_bibdoc FROM bibrec_bibdoc WHERE type=%s",(atype,))
    for row in res:
        id_bibrec, id_bibdoc = row
        abibdoc = BibDoc(recid=id_bibrec, docid=id_bibdoc)
        abibdoc_type_pathname = os.path.join(abibdoc.get_base_dir(), '.type')
        # update DB value:
        run_sql("""UPDATE bibrec_bibdoc SET type=%s WHERE type=%s AND id_bibrec=%s AND id_bibdoc=%s""", 
        (btype, atype, id_bibrec, id_bibdoc))
        # update file value:
        fdesc = open(abibdoc_type_pathname, 'w')
        fdesc.write(btype)
        fdesc.close()
        # print info
        print "I: updated record %s file %s" % (id_bibrec, abibdoc_type_pathname)