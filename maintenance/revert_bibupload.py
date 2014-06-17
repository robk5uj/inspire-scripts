#! /usr/bin/env python
## -*- mode: python; coding: utf-8; -*-
##
## This file is part of CDS Invenio.
## Copyright (C) 2002, 2003, 2004, 2005, 2006, 2007 CERN.
##
## CDS Invenio is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as
## published by the Free Software Foundation; either version 2 of the
## License, or (at your option) any later version.
##
## CDS Invenio is distributed in the hope that it will be useful, but
## WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
## General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with CDS Invenio; if not, write to the Free Software Foundation, Inc.,
## 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
"""
Revert all records mentioned in MARCXML to previous version
"""
__revision__ = "$Id:$"

import sys
import getopt
import string
import re
import tempfile

from invenio.bibtask import task_low_level_submission
from invenio.dbquery import run_sql
from invenio.bibrecord import create_records
from invenio.config import CFG_TMPSHAREDDIR
from invenio.bibedit_utils import get_marcxml_of_revision_id, get_record_revision_ids

def get_previous_xml(recids):
    """
    Return the previous MARCXML of each given recid, as well as list
    of recids that do not have a previous revision
    """
    recids_without_previous_rev = []
    prev_xml = ''
    for recid in recids:
        revisions = get_record_revision_ids(recid)
        if len(revisions) > 1:
            revid = revisions[-2]
            prev_xml += get_marcxml_of_revision_id(revid)
        else:
            recids_without_previous_rev.append(recid)

    if prev_xml:
        prev_xml = "<collection>\n" + prev_xml + "</collection>"

    return (prev_xml, recids_without_previous_rev)

def bibupload_xml(xml, username="admin"):
    """
    Upload the given marcxml via BibUpload
    """
    try:
        (h, file_path) = tempfile.mkstemp(prefix='revert_marcxml_to_previous_version_', suffix='.xml', dir=CFG_TMPSHAREDDIR)
        fd = open(file_path, 'w')
        fd.write(xml)
        fd.close()
    
        task_id = task_low_level_submission('bibupload', 'revert', '-P', '5', '-r',
                                            file_path, '-u', username)
        return "Task #%s scheduled with MARCXML at %s" % (task_id, file_path,)

    except Exception, e:
        print "The updates could not be uploaded:\n%s" % e
        return ''

done_recids_re = re.compile("--> Record (\d+) DONE")


def usage(exit_status=1):
    """
    Print usage and exit.
    """

    print """Usage: %s [OPTIONS] < input.xml [options]
    Specific options:
      -u, --user            Username for uploading changes (Default: admin)
      -p, --pretend         Print output instead of uploading
    Mandatory options:
      -l, --log             Input is a log file
      -x, --xml             Input is a MARCXML
    General options:
      -h, --help            Print this help.
      -V, --version         Print version information.
      -v, --verbose=LEVEL   Verbose level (from 0 to 9, default 0).

    Description: Revert to previous version each records in given XML or log file.
    NOTE: IT DOES SIMPLY REVERT TO CURRENT VERSION - 1, and does not try to revert
    to version before the update corresponding to the input file (i.e. might not
    give expected result if other updates have been run on the the records).

    Examples:
     $ sudo -u apache ./revert_bibupload.py -p -x -u anne   < /opt/cdsweb/var/tmp-shared/batchupload__ZkTjcu.xml
     $ sudo -u apache ./revert_bibupload.py -p -l -u tullio < /opt/cdsweb/var/log/bibsched_task_940381.log
     $ sudo -u apache ./revert_bibupload.py -l < /opt/cdsweb/var/log/bibsched_task_940381.log     
    """ % (sys.argv[0])
    sys.exit(exit_status)

def main():
    """
    Read input MARCXML or log file, identify recids and restore previous version of each record
    """
    verbose = 0
    recids = []
    username = 'admin'
    input_file_type = None
    pretend_p = False
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hVv:xlu:p",
                                   ["help",
                                    "version",
                                    "verbose=",
                                    "xml",
                                    "log",
                                    "user=",
                                    "pretend"])
    except getopt.GetoptError:
        return usage(2)

    for opt in opts:
        if opt[0] in ("-V","--version"):
            print __revision__
            sys.exit(0)
        elif opt[0] in ("-h","--help"):
            return usage(0)
        elif opt[0] in ("-v", "--verbose"):
            try:
                verbose = string.atoi(opt[1])
            except ValueError:
                print "Error: verbose must be an integer."
                return usage(2)
        elif opt[0] in ("-l", "--log"):
            if input_file_type == "xml":
                print "Cannot specify both options --log and --xml"
                return usage(0)
            input_file_type = "log"
        elif opt[0] in ("-x", "--xml"):
            if input_file_type == "log":
                print "Cannot specify both options --log and --xml"
                return usage(0)
            input_file_type = "xml"
        elif opt[0] in ("-u", "--user"):
            username = opt[1]
        elif opt[0] in ("-p", "--pretend"):
            pretend_p = True


    if not input_file_type:
        print "You must specify type of input file with --log or --xml"
        return usage(0)

    if not sys.stdin.isatty():
        input_stream = sys.stdin.read()
        if not input_stream:
            print "Given input file is empty"
        if input_file_type == "xml":
            # read marcxml
            records = create_records(input_stream)
            recids = [int(r[0]['001'][0][3]) for r in records]
        else:
            # read log file
            recids = [int(matchobj.group(1)) for matchobj in done_recids_re.finditer(input_stream)]
        (previous_marcxml, recids_without_previous_rev) = get_previous_xml(recids)
        if not pretend_p:
            sys.stdin = open('/dev/tty')
            if not recids_without_previous_rev:
                confirm_p = raw_input("Identified %i records(s) to revert. Continue and upload? [y/N]" % len(recids))
            else:
                confirm_p = raw_input("Identified %i records(s) to revert, but only %i had a previous revision (hint: check with '--pretend').\nAre you sure you want to continue [y/N/d(isplay recids without revision)]" % (len(recids), len(recids_without_previous_rev)))
            if confirm_p == "y":
                print bibupload_xml(previous_marcxml, username)
            elif confirm_p == 'd':
                print ', '.join(recids_without_previous_rev)
            else:
                print 'Aborted'
        else:
            print previous_marcxml
    else:
        print "No input file specified"
        return usage(0)

if __name__ == "__main__":
    main()
