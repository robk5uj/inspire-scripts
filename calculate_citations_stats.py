import time

from invenio.bibrank_citation_searcher import get_cited_by
from invenio.search_engine_utils import get_fieldvalues
from invenio.dateutils import strptime


def find_year(recordyear):
    """find the year in the string as a suite of 4 int"""
    s = ""
    for i in range(len(recordyear)-3):
        s = recordyear[i:i+4]
        if s.isalnum():
            break
    return int(s)


def calculate_citation_graphe_x_coordinates(recid):
    """Return a range of year from the publication year of record RECID
       until the current year."""
    recordyear = get_fieldvalues(recid, '269__c')
    if not recordyear:
        recordyear = get_fieldvalues(recid, '773__y')
        if not recordyear:
            recordyear = get_fieldvalues(recid, '260__c')

    currentyear = time.localtime()[0]

    if recordyear == []:
        recordyear = currentyear
    else:
        recordyear = find_year(recordyear[0])

    return range(recordyear, currentyear+1)


def calculate_citation_history_coordinates(recid):
    """Return a list of citation graph coordinates for RECID, sorted by year."""
    result = {}
    for year in calculate_citation_graphe_x_coordinates(recid):
        result[year] = 0

    for recid in get_cited_by(recid):
        rec_date = get_fieldvalues(recid, '269__c')
        if not rec_date:
            rec_date = get_fieldvalues(recid, '773__y')
            if not rec_date:
                rec_date = get_fieldvalues(recid, '260__c')
        # Some records simlpy do not have these fields
        if rec_date:
            # Maybe rec_date[0][0:4] has a typo and cannot
            # be converted to an int
            try:
                d = strptime(rec_date, '%Y-%m')
            except ValueError:
                pass
            else:
                if d.year in result:
                    result[d.year] += 1

    return sorted(result.iteritems())
