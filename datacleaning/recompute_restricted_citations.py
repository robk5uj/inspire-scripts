from invenio.search_engine import get_collection_reclist
from invenio.bibtask import task_low_level_submission


PREFIX = 'restricted-citations'

RESTRICTED_COLLECTIONS = (
    'H1 Internal Notes',
    'HERMES Internal Notes',
    'ZEUS Internal Notes',
    'D0 Internal Notes',
    'ZEUS Preliminary Notes',
    'H1 Preliminary Notes',
)


def submit_bibrank(to_submit, prefix):
    return task_low_level_submission('bibrank', prefix,
                                     '-w', 'citation',
                                     '-i', ','.join(str(i) for i in to_submit))



for coll in RESTRICTED_COLLECTIONS:
    recids = get_collection_reclist(coll)
    submit_bibrank(recids, '%s-%s' % (PREFIX, coll.lower().replace(' ', '-')))
