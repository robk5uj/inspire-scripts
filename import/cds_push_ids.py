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
import sys
import requests

from invenio.search_engine import (perform_request_search,
                                   get_record)
from invenio.bibrecord import (record_get_field_instances,
                               create_record,
                               record_add_field,
                               record_xml_output)

regex_rec = re.compile('<record.*?>.*?</record>', re.DOTALL)


def retrieve_record(baseurl, recid, of):
    """
    Read all lines of given url request. Returns a list of lines.
    """
    url = "%s/record/%s/export/%s" % (baseurl, recid, of)
    print url
    r = requests.get(url)
    if of == "xm":
        try:
            return regex_rec.search(r.text).group()
        except AttributeError:
            print(r.text)
            print("ERROR: timeout")
            time.sleep(10.0)
            return retrieve_record(baseurl, recid, of)
    else:
        return r.text


def record_get_value_with_provenence(record, tag, ind1=" ", ind2=" ",
                                     value_code="", provenence_code="9",
                                     provenence_value="CDS"):
    """
    Retrieves the value of the field with given provenence.
    """
    fields = record_get_field_instances(record, tag, ind1, ind2)
    final_values = []
    for subfields, dummy1, dummy2, dummy3, dummy4 in fields:
        for code, value in subfields:
            if code == provenence_code and value.lower() == provenence_value.lower():
                # We have a hit. Stop to look for right value
                break
        else:
            # No hits.. continue to next field
            continue
        for code, value in subfields:
            if code == value_code:
                # This is the value we are looking for with the correct provenence
                final_values.append(value)
    return final_values


def main():
    """
    """
    query = "980:THESIS and 035:CDS and (datecreated:2012 or datecreated:2013 or datecreated:2014)"
    res = perform_request_search(p=query, c="HEP", of="id")

    id_map = {}

    for recid in res:
        record = get_record(recid)
        cds_id = record_get_value_with_provenence(record, "035",
                                                  value_code="a",
                                                  provenence_code="9",
                                                  provenence_value="CDS")
        if cds_id:
            cds_id = cds_id[0]
            print("Found CDS: %s" % (cds_id,))
            rec = retrieve_record("http://cds.cern.ch", cds_id, "xm")
            time.sleep(1.0)
            cds_rec = create_record(rec)[0]
            inspire_id = record_get_value_with_provenence(cds_rec, "035",
                                                          value_code="a",
                                                          provenence_code="9",
                                                          provenence_value="INSPIRE")
            if inspire_id:
                inspire_id = inspire_id[0]
                print("Found INSPIRE: %s" % (inspire_id,))
            else:
                print("Adding INSPIRE ID")
                id_map[cds_id] = record["001"][0][3]

    print(id_map)

    # Hack to activate UTF-8
    reload(sys)
    sys.setdefaultencoding("utf8")
    assert sys.getdefaultencoding() == "utf8"

    out_fd, out_name = mkstemp(".xml", "cds_upload_mapping_")
    os.close(out_fd)

    with open(out_name, "w") as xml_out:
        xml_out.write("<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n")

        for cds_id, inspire_id in id_map.items():
            rec = {}
            record_add_field(rec, '001', controlfield_value=cds_id)
            record_add_field(rec, '035', subfields=[('9', "Inspire"),
                                                    ('a', inspire_id)])
            xml_out.write(record_xml_output(rec))
        xml_out.write("</collection>\n")

    print "XML ready completed. Find results here: %s" % (out_name,)

if __name__ == "__main__":
    main()
