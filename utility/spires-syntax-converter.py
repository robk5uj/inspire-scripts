#!/usr/bin/python
#
# TS 2/2014
"""
   SPIRES to INVENIO search syntax Conversion

   Script to introspect invenio.search_engine_query_parser handling 
   of SPIRES style searches.
"""

from invenio.search_engine_query_parser import SpiresToInvenioSyntaxConverter
import sys

def convert_to_invenio_syntax(spiressearch=None):
    """
    Convert a SPIRES style search string to the equivalent INVENIO style search

    Parameters:

      - 'spiressearch' *string* - the SPIRES style search string

    Return Value:

      - *string* - the equivalent INVENIO search string
    """

    if not spiressearch:
        return 'no search string provided'
    invenio_search = SpiresToInvenioSyntaxConverter().convert_query(spiressearch)
    return invenio_search

def main(searchstring=None):
    """
    Display information on SPIRES search conversion to INVENIO search

    Parameters:
      - 'searchstring' *string* - the SPIRES style search string
    """

    if not searchstring:
        print "No SPIRES like search phrase provided"
        return None
    res = convert_to_invenio_syntax(searchstring)
    if res:
        print """
        The Spires style search string you entered

        \t[%s]

        is interpreted in invenio search as

        \t[%s]
    
        """ % (searchstring, res)
    else:
        print 'Syntax conversion for >>>%s<<< failed!' % (searchstring)
    return 0

if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            main(str(sys.argv[1]))
        except KeyboardInterrupt:
            print 'Exiting on keyboard interrupt'
    else:
        print """
        not enough arguments to work with, use as:

        \t sys.argv[0] "<SPIRES style search string>"

        """
