#!/usr/bin/python

from invenio.search_engine import run_sql, search_unit
from invenio.intbitset import intbitset
from datetime import datetime, timedelta
import sys

def main(exactauthor, days):
    """
    analyse the rnkCITATIONLOG table and print out a list of all citer-citee
    recid pairs sorted by action_date, where citee records all are by the author
    provided, which where removed in the past n days

    parameters:
    * exactauthor - (str) the BAI author identifier
    * days - (int) how many days to look back from now()
    """

    if days.isdigit():
        days = int(days)
    else:
        print "WARNING: number of days not recognized as integer, using 1 instead"
        days = 1
    startdate = datetime.today() - timedelta(days=days)

    print "\n* Looking at citation loss for %s since %s *\n" \
        % (exactauthor, startdate)

    result = citationloss(exactauthor, startdate)
    if result:
        create_report(result, days)
    else:
        print "nothing lost in the past %d days" % days
    print "\n* ALL DONE *\n"

def citationloss(exactauthor, startdate):
    """
    analyse the rnkCITATIONLOG table for records belonging to exactauthor which
    have had citations removed since startdate
    """

    recordsofauthor = search_unit(exactauthor, f='exactauthor')
    removedcitations = intbitset([i[0] for i in \
                                  run_sql('select citee from rnkCITATIONLOG where action_date>"%s" and type="removed"' % startdate)])

    lossoverlap = recordsofauthor & removedcitations
    if lossoverlap:
        recsaffected = run_sql(
            'select citer,citee,action_date from rnkCITATIONLOG' \
            + ' where citee in (%s) and action_date>"%s" and type="removed"' \
            % (', '.join([str(i) for i in lossoverlap]), startdate)
        )
        return recsaffected
    return None

def create_report(recsaffected, days):
    """
    print results in a nicely formatted form
    """

    srecsaffected = sorted(recsaffected, key=lambda k: k[2])
    print 'citer\tcitee\t   action_date\n' + '-'*40
    count = 0
    for row in srecsaffected:
        print "%d\t%d\t%s" % row
        count += 1
    print "\n\n* total number of removed citations in the past %s days: %s *" % (days, count)

if __name__ == "__main__":
    if len(sys.argv) > 2:
        try:
            main(str(sys.argv[1]), str(sys.argv[2]))
        except KeyboardInterrupt:
            print 'Exiting on keyboard interrupt'
    else:
        print """
        not enough arguments to work with, use as:

        \t %s "<exactauthor> <number of days to look back>"

        """ % sys.argv[0]
