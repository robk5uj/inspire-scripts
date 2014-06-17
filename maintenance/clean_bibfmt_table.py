"""
Used before adding new primary key in bibfmt
"""
from invenio.dbquery import run_sql


def get_duplicate_rows():
    """ Perform table cleanup """
    res = run_sql("""
                  select count(id_bibrec) as cnt, id_bibrec, format, last_updated
                  from bibfmt
                  where format='recstruct'
                  group by id_bibrec having cnt > 1
                  """, with_dict=True)
    return res


def delete_row(table, options):
    sql = "DELETE FROM %s " % (table,)
    #sql = "SELECT id_bibrec FROM %s " % (table,)
    if options:
        sql += "WHERE "
        for key, value in options.items():
            try:
                value = int(value)
            except (ValueError, TypeError):
                value = "'%s'" % (value,)
            sql += "%s=%s AND " % (key, str(value))
        sql = sql[:-5]
    sql += " LIMIT 1"
    print sql
    res = run_sql(sql)
    print res


def clean():
    for row in get_duplicate_rows():
        del row["cnt"]
        delete_row("bibfmt", row)


if __name__ == "__main__":
    clean()
