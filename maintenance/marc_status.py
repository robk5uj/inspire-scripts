#!/usr/bin/env python

import sys

from invenio.dbquery import run_sql
from invenio.search_engine import get_tag_name
from invenio.search_engine import get_collection_reclist
from invenio.intbitset import intbitset

collection = sys.argv[1]

recids = get_collection_reclist(collection)

for i in range(100):
    for tag in run_sql("SELECT DISTINCT tag FROM bib%02dx ORDER BY tag" % i):
        tag = tag[0]
        tag_name = get_tag_name(tag)
        new_tag = True
        recids_with_value = recids & intbitset(run_sql("SELECT id_bibrec from bibrec_bib%02dx JOIN bib%02dx ON id_bibxxx=id WHERE tag=%%s" % (i, i), (tag, )))
        if not recids_with_value:
            continue
        distinct_values = run_sql("SELECT COUNT(1) FROM bib%02dx WHERE tag=%%s" % i, (tag, ))[0][0]
        print
        msg =  "%s (%s), (%s %s records with values) (%s distinct values in general)" % (tag, tag_name, len(recids_with_value), collection, distinct_values)
        msg2 = str(recids_with_value)
        print msg
        print msg2
        print "-" * max(len(msg), len(msg2))

        distinct_values = run_sql("SELECT COUNT(1) FROM bib%02dx WHERE tag=%%s" % i, (tag, ))[0][0]

        if distinct_values > 1000:
            # Too much populated query.
            continue
        limit = 10
        if distinct_values < 30:
            limit = distinct_values
        outliers = run_sql("SELECT value, count(*) AS c, id FROM bibrec_bib%02dx join bib%02dx ON id_bibxxx=id WHERE tag=%%s GROUP BY id ORDER BY c LIMIT %%s" % (i, i), (tag, limit))

        for value, dummy_count, id in outliers:
            matched_recids = recids & intbitset(run_sql("SELECT id_bibrec from bibrec_bib%02dx WHERE id_bibxxx=%%s" % i, (id, )))
            if matched_recids:
                print "%s (%s %s records): %s" % (value, len(matched_recids), collection, matched_recids)
