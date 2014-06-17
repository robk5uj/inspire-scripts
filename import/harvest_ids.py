#!/usr/bin/python
## This file is part of INSPIRE-HEP.
## Copyright (C) 2013 CERN.
##
## INSPIRE-HEP is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as
## published by the Free Software Foundation; either version 2 of the
## License, or (at your option) any later version.
##
## INSPIRE-HEP is distributed in the hope that it will be useful, but
## WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
## General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with INSPIRE-HEP; if not, write to the Free Software Foundation, Inc.,
## 59 Temple Place, Suite 330, Boston, MA 021111307, USA.

from tempfile import mkstemp
import os
import re
import time
import getopt
import sys
import requests

regex_rec = re.compile('<record.*?>.*?</record>', re.DOTALL)


def retrieve_record(baseurl, recid, of):
    """
    Read all lines of given url request. Returns a list of lines.
    """
    url = "%s/record/%s/export/%s" % (baseurl, recid, of)
    r = requests.get(url)
    r.encoding = "UTF-8"
    if of == "xm":
        return regex_rec.search(r.text).group()
    else:
        return r.text


def main():
    usage = """
    harvest.py [-f of] [-s url]

    Example:
    $ python harvest_ids.py -s 'http://cds.cern.ch' -f 'xm' [FILE_WITH_IDS]
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hf:t:s:", [])
    except getopt.GetoptError, e:
        sys.stderr.write("Error:" + str(e) + "\n")
        print usage
        sys.exit(1)

    server_url = "http://inspirehep.net"
    of = "xm"
    sleep = 1.0
    filename = args[0]

    for opt, opt_value in opts:
        if opt in ['-f']:
            of = opt_value
        if opt in ['-t']:
            sleep = float(opt_value)
        if opt in ['-s']:
            server_url = opt_value
        if opt in ['-h']:
            print usage
            sys.exit(0)

    recids_to_harvest = []
    for recid in open(filename):
        recid = recid.strip()
        recids_to_harvest.append(recid)

    recids_to_harvest = set(recids_to_harvest)
    print "Found %i records to harvest." % (len(recids_to_harvest))

    # Hack to activate UTF-8
    reload(sys)
    sys.setdefaultencoding("utf8")
    assert sys.getdefaultencoding() == "utf8"

    results = []
    out_fd, out_name = mkstemp(".out", "harvest_")
    if of == "xm":
        os.write(out_fd, "<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n")

    for recid in recids_to_harvest:
        record = retrieve_record(server_url, recid, of)
        results.append(record)
        os.write(out_fd, record.encode("utf-8"))
        print "Harvested %s" % (recid,)
        time.sleep(sleep)

    if of == "xm":
        os.write(out_fd, "</collection>\n")
    print "Harvested %i records" % (len(results),)
    os.close(out_fd)

    print "Harvest completed. Find results here: %s" % (out_name,)

if __name__ == "__main__":
    main()
