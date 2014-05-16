#!/usr/bin/env python
## This file is part of INSPIRE-HEP.
## Copyright (C) 2014 CERN.
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

import os
import getopt
import sys
import re
from time import sleep
from random import randint

from invenio.bibformat_engine import BibFormatObject
from invenio.filedownloadutils import download_url
from invenio.bibrecord import (record_get_field_instances,
                               record_add_field,
                               field_get_subfield_values,
                               create_record)


regex_rec = re.compile('<record.*?>.*?</record>', re.DOTALL)


def write_record_to_file(filename, record_list):
    """Writes a new MARCXML file to specified path from BibRecord list."""
    from invenio.bibrecord import record_xml_output

    if len(record_list) > 0:
        out = []
        out.append("<collection>")
        for record in record_list:
            if record != {}:
                out.append(record_xml_output(record))
        out.append("</collection>")
        if len(out) > 2:
            file_fd = open(filename, 'w')
            file_fd.write("\n".join(out))
            file_fd.close()
            return True


def main():
    usage = """
    harvest.py [-f of] [-s url]

    Example:
    $ python download_fft_files_locally.py -d [location] [FILE_WITH_IDS]
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd:o:", [])
    except getopt.GetoptError, e:
        sys.stderr.write("Error:" + str(e) + "\n")
        print usage
        sys.exit(1)

    try:
        filename = args[0]
    except Exception:
        print usage
        sys.stderr.write("Error: No filename\n")
        sys.exit(1)

    file_directory = "/tmp"
    out_directory = os.path.dirname(filename)

    for opt, opt_value in opts:
        if opt in ['-d']:
            file_directory = opt_value
        if opt in ['-o']:
            out_directory = opt_value
        if opt in ['-h']:
            print usage
            sys.exit(0)

    print("Got directories: %r" % ([file_directory, out_directory],))

    # Hack to activate UTF-8
    reload(sys)
    sys.setdefaultencoding("utf8")
    assert sys.getdefaultencoding() == "utf8"

    out_records = []
    for rec in regex_rec.findall(open(filename).read()):
        record = create_record(rec)[0]
        fft_fields = record_get_field_instances(record, "FFT")
        if not fft_fields:
            continue

        del record["FFT"]

        for fft_field in fft_fields:
            url = field_get_subfield_values(fft_field, "a")[0]
            prefix = randint(100000, 999999999)
            download_to_file = os.path.join(
                file_directory,
                "%s_" % (prefix,) + os.path.basename(url)
            )
            try:
                download_url(url, download_to_file=download_to_file)
                sleep(2.0)
            except Exception:
                print("Passed on %s" % (url,))
                continue
            print("Downloaded: %s" % (download_to_file,))

            new_subfields = []
            for subfield in fft_field[0]:
                if "a" in subfield:
                    subfield = ("a", download_to_file)
                new_subfields.append(subfield)
            new_subfields.append(("n", ".".join(os.path.basename(url).split(".")[:-1])))
            print new_subfields
            record_add_field(record, "FFT", subfields=new_subfields)
        out_records.append(record)

    new_filename = os.path.join(
        out_directory,
        os.path.basename(filename) + ".localfft.xml"
    )
    write_record_to_file(new_filename, out_records)
    print("Wrote %s" % (new_filename,))


if __name__ == "__main__":
    main()
