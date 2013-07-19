from invenio.search_engine import get_record
from invenio.textutils import encode_for_xml
from invenio.dbquery import run_sql
from invenio.intbitset import intbitset
from invenio.bibupload import delete_bibrec_bibxxx, update_database_with_metadata
from invenio.bibrecord import create_record, record_add_field

def identical_records(rec1, rec2, skip_005=True, ignore_subfield_order=False, ignore_duplicate_subfields=False, ignore_duplicate_controlfields=False):
    """
    Return True if rec1 is identical to rec2, regardless of a difference
    in the 005 tag (i.e. the timestamp).
    """
    rec1_keys = set(rec1.keys())
    rec2_keys = set(rec2.keys())
    if skip_005:
        rec1_keys.discard("005")
        rec2_keys.discard("005")
    if rec1_keys != rec2_keys:
        return False
    for key in rec1_keys:
        ## We sort the fields, first by indicators, then by global position and then by anything else
        if ignore_duplicate_controlfields and key.startswith('00'):
            if set(field[3] for field in rec1[key]) != set(field[3] for field in rec2[key]):
                return False
            continue

        rec1_fields = rec1[key]
        rec2_fields = rec2[key]
        if len(rec1_fields) != len(rec2_fields):
            ## They already differs in length...
            return False
        rec1_fields = sorted(rec1_fields, key=lambda elem: (elem[1], elem[2], elem[4], elem[3], elem[0]))
        rec2_fields = sorted(rec2_fields, key=lambda elem: (elem[1], elem[2], elem[4], elem[3], elem[0]))
        for field1, field2 in zip(rec1_fields, rec2_fields):
            if ignore_duplicate_subfields:
                if field1[1:4] != field2[1:4] or set(field1[0]) != set(field2[0]):
                    return False
            elif ignore_subfield_order:
                if field1[1:4] != field2[1:4] or sorted(field1[0]) != sorted(field2[0]):
                    return False
            elif field1[:4] != field2[:4]:
                return False
    return True

def get_record_from_bibxxx(recid):
    rec = {}
    record_add_field(rec, '001', controlfield_value=str(recid))
    query = "SELECT b.tag,b.value FROM bib00x AS b, bibrec_bib00x AS bb "\
            "WHERE bb.id_bibrec=%s AND b.id=bb.id_bibxxx AND b.tag LIKE '00%%' "\
            "ORDER BY bb.field_number, b.tag ASC"
    res = run_sql(query, (recid, ))
    for field, value in res:
        record_add_field(rec, field[:3], controlfield_value=value)

    for digit1 in range(0, 10):
        for digit2 in range(0, 10):
            if digit1 == 0 and digit2 == 0:
                continue
            bx = "bib%d%dx" % (digit1, digit2)
            bibx = "bibrec_bib%d%dx" % (digit1, digit2)
            query = "SELECT b.tag,b.value,bb.field_number FROM %s AS bb JOIN %s AS b ON b.id=bb.id_bibxxx "\
                    "WHERE bb.id_bibrec=%%s AND b.tag LIKE %%s"\
                    "ORDER BY bb.field_number, b.tag ASC" % (bibx, bx)
            res = run_sql(query, (recid, str(digit1)+str(digit2)+'%'))
            previous_tag = ""
            previous_field_number = -1
            subfields = []
            for tag, value, field_number in res:
                if previous_tag[:5] != tag[:5] or previous_field_number != field_number:
                    if previous_field_number > -1 and previous_tag and subfields:
                        record_add_field(rec, previous_tag[:3], ind1=previous_tag[3], ind2=previous_tag[4], subfields=subfields)
                    subfields = []
                subfields += [(tag[5], value)]
                previous_tag = tag
                previous_field_number = field_number
            if previous_field_number > -1 and previous_tag and subfields:
                record_add_field(rec, previous_tag[:3], ind1=previous_tag[3], ind2=previous_tag[4], subfields=subfields)
    return rec

def get_record_from_bibxxx2(recid):
    """Return a recstruct built from bibxxx tables"""
    record = "<record>"
    record += """        <controlfield tag="001">%s</controlfield>\n""" % recid
    # controlfields
    query = "SELECT b.tag,b.value,bb.field_number FROM bib00x AS b, bibrec_bib00x AS bb "\
            "WHERE bb.id_bibrec=%s AND b.id=bb.id_bibxxx AND b.tag LIKE '00%%' "\
            "ORDER BY bb.field_number, b.tag ASC"
    res = run_sql(query, (recid, ))
    for row in res:
        field, value = row[0], row[1]
        value = encode_for_xml(value)
        record += """        <controlfield tag="%s">%s</controlfield>\n""" % \
                (encode_for_xml(field[0:3]), value)
    # datafields
    i = 1 # Do not process bib00x and bibrec_bib00x, as
            # they are controlfields. So start at bib01x and
            # bibrec_bib00x (and set i = 0 at the end of
            # first loop)
    for digit1 in range(0, 10):
        for digit2 in range(i, 10):
            bx = "bib%d%dx" % (digit1, digit2)
            bibx = "bibrec_bib%d%dx" % (digit1, digit2)
            query = "SELECT b.tag,b.value,bb.field_number FROM %s AS b, %s AS bb "\
                    "WHERE bb.id_bibrec=%%s AND b.id=bb.id_bibxxx AND b.tag LIKE %%s"\
                    "ORDER BY bb.field_number, b.tag ASC" % (bx, bibx)
            res = run_sql(query, (recid, str(digit1)+str(digit2)+'%'))
            field_number_old = -999
            field_old = ""
            for row in res:
                field, value, field_number = row[0], row[1], row[2]
                ind1, ind2 = field[3], field[4]
                if ind1 == "_" or ind1 == "":
                    ind1 = " "
                if ind2 == "_" or ind2 == "":
                    ind2 = " "
                if field_number != field_number_old or field[:-1] != field_old[:-1]:
                    if field_number_old != -999:
                        record += """        </datafield>\n"""
                    record += """        <datafield tag="%s" ind1="%s" ind2="%s">\n""" % \
                                (encode_for_xml(field[0:3]), encode_for_xml(ind1), encode_for_xml(ind2))
                    field_number_old = field_number
                    field_old = field
                # print subfield value
                value = encode_for_xml(value)
                record += """            <subfield code="%s">%s</subfield>\n""" % \
                    (encode_for_xml(field[-1:]), value)

            # all fields/subfields printed in this run, so close the tag:
            if field_number_old != -999:
                record += """        </datafield>\n"""
        i = 0 # Next loop should start looking at bib%0 and bibrec_bib00x
    # we are at the end of printing the record:
    record += "    </record>\n"
    return create_record(record)[0]

def check_record_consistency(recid):
    rec = get_record(recid)
    rec_in_bibxxx = get_record_from_bibxxx(recid)
    return identical_records(rec, rec_in_bibxxx, skip_005=True, ignore_duplicate_controlfields=True, ignore_duplicate_subfields=True, ignore_subfield_order=True)

def delete_bibrec_bibxxx(recid):
    for i in range(0, 10):
        for j in range(0, 10):
            run_sql("DELETE FROM %(bibrec_bibxxx)s WHERE id_bibrec=%%s" %  # kwalitee: disable=sql
                    {'bibrec_bibxxx': "bibrec_bib%i%ix" % (i, j)},
                    (recid,))

def fix_broken_record(recid):
    record = get_record(recid)
    delete_bibrec_bibxxx(recid)
    update_database_with_metadata(record, recid, oai_rec_id=None)
    run_sql("UPDATE bibrec SET modification_date=NOW() WHERE id=%s", (recid, ))


def main():
    CFG_AFFECTED_RECORDS = intbitset([int(recid) for recid in open("recids_to_correct")])

    out = open("recids_to_correct2", "w")

    for recid in CFG_AFFECTED_RECORDS:
        try:
            if not check_record_consistency(recid):
                print "Correct recid %s" % recid
                fix_broken_record(recid)
                print >> out, recid
            else:
                print "recid %s is good!" % recid
        except Exception, err:
            print "Exception: catpured with recid %s: %s" % (recid, err)

if __name__ == "__main__":
    main()

