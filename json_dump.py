from datetime import datetime

try:
    import json
except ImportError:
    import simplejson as json


from invenio.search_engine import get_collection_reclist
from invenio.docextract_record import get_record
from invenio.bibrank_citation_searcher import (get_refers_to,
                                               get_cited_by)


def get_creation_date(record):
    date = record.find_subfields('269__c')
    if not date:
        date = record.find_subfields('260__c')
        if not date:
            date = record.find_subfields('773__y')
            if not date:
                date = record.find_subfields('502__d')
    if date:
        return date[0].value
    else:
        return None


def prepare_record(recid):
    record = get_record(recid)
    try:
        title = record.find_subfields('245__a')[0].value
    except IndexError:
        title = ''
    record_json = {'recid': recid,
                   'title': title,
                   'authors': [f.value for f in record.find_subfields('100__a')],
                   'co-authors': [f.value for f in record.find_subfields('700__a')],
                   'creation_date': get_creation_date(record),
                   'references': list(get_refers_to(recid)),
                   'citations': list(get_cited_by(recid))}
    return record_json


if __name__ == '__main__':
    recids = get_collection_reclist('HEP')
    with open('records.json', 'w') as out:
        for recid in recids:
            print 'writing', recid
            json.dump(prepare_record(recid), out)
            out.write('\n')
            out.flush()
    # 'date_generated': datetime.now().isoformat()},
