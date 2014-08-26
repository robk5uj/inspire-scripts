#!/usr/bin/env python

from invenio.dbquery import run_sql

def main():
    sigs = run_sql("select personid, bibref_table, bibref_value, bibrec, flag from aidPERSONIDPAPERS where flag>-2")
    # 0 1 2 3 4

    signatures = set((x[1],x[2],x[3]) for x in sigs)
    print "Signatures:", len(signatures)

    records = set(x[3] for x in sigs)
    print "Records:", len(records)

    claimed_profiles = set(x[0] for x in sigs if int(x[4]) == 2)
    print "Claimed profiles:", len(claimed_profiles)

    claimed_signatures = set((x[1],x[2],x[3]) for x in sigs if int(x[4]) == 2)
    print "Claimed signatures:", len(claimed_signatures)

    claimed_records = set(x[3] for x in sigs if int(x[4]) == 2)
    print "Claimed records:", len(claimed_records)

    arxiv_logins = run_sql("select * from aidPERSONIDDATA where tag='uid'")
    print "Profiles with Arxiv logins:", len(arxiv_logins)

    orcids = run_sql("select * from aidPERSONIDDATA where tag='extid:ORCID'")
    print "Profiles with orcids:", len(orcids)

    inspireid = run_sql("select * from aidPERSONIDDATA where tag='extid:inspireid'")
    print "Profiles with Inspire-ID:", len(inspireid)

    profiles = set(x[0] for x in run_sql("select personid from aidPERSONIDPAPERS"))
    print "Profiles in total:", len(profiles)

if __name__ == "__main__":
    main()
