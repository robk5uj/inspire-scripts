from invenio.intbitset import intbitset
from invenio.bibrank_citation_searcher import get_cited_by
from invenio.search_engine import search_pattern
from invenio.search_engine import get_collection_reclist

RESTRICTED_COLLECTIONS = (
    'H1 Internal Notes',
    'HERMES Internal Notes',
    'ZEUS Internal Notes',
    'D0 Internal Notes',
    'ZEUS Preliminary Notes',
    'H1 Preliminary Notes',
)

restricted_recids = intbitset()
for coll in RESTRICTED_COLLECTIONS:
    restricted_recids += get_collection_reclist(coll)
print 'Total restricted papers: %s' % len(restricted_recids)

counter = 0

recids = search_pattern(p='exactauthor:A.D.Polosa.1')
for recid in recids:
    for citer in get_cited_by(recid):
        if citer in restricted_recids:
            counter += 1

print 'Total restricted citations: %s' % counter
