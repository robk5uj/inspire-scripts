from os.path import join
from cPickle import loads
from zlib import decompress
from invenio.bibcheck_task import AmendableRecord
from invenio.bibrecord import record_get_field_instances, field_get_subfield_instances, record_xml_output
from invenio.search_engine import perform_request_search, get_record
from job_helper import ChunkedBibUpload
from invenio.config import CFG_LOGDIR
from invenio.errorlib import get_pretty_traceback

CFG_ALICE_MAP = loads(decompress(open("/afs/cern.ch/project/inspire/alice_map.blob").read()))

recids = perform_request_search(p='8564_u:"http://alice.cern.ch/format/showfull?sysnb=*" and 8564_w:0->9"')

bibupload = ChunkedBibUpload(wait_for_task=False, mode='c', user="kaplun", name="alice2cds", notimechange=True)
bibupload.chunk_size = 5000

log = open(join(CFG_LOGDIR, "alice2cds.log"), "a")
tmp_output = open(join(CFG_LOGDIR, "alice2cds.xml"), "w")

for i, recid in enumerate(recids):
    if i % 100 == 0:
        print "%s%%" % (i * 100 / len(recids)), i
    print >> log, "Processing record %s" % recid
    try:
        record = AmendableRecord(get_record(recid))
        record.rule = {'name': 'alice2cds', 'holdingpen': False}
        sysno = None
        for position, value in record.iterfield("8564_u"):
            if value.startswith("http://alice.cern.ch/format/showfull?sysnb="):
                sysno = int(value.split('=')[1])
                print >> log, "DEBUG: found sysno: %s" % sysno
        if not sysno:
            print >> log, "WARNING: skipping record %s: can't sysno" % (recid)
            continue

        if not CFG_ALICE_MAP.get(sysno, []):
            print >> log, "WARNING: skipping record %s: can't find CDS id for %09d" % (recid, sysno)
            continue

        cds_recids = CFG_ALICE_MAP[sysno]
        if len(cds_recids) > 1:
            print >> log, "WARNING: skipping record %s: more than 1 recid found on CDS id for %09d: %s" % (recid, sysno, cds_recids)
            continue

        cds_recid = cds_recids[0]
        print >> log, "DEBUG: found cds_recid: %s" % cds_recid
        fields = record_get_field_instances(record, '035')
        for field in fields:
            subfields = field_get_subfield_instances(field)
            if ('9', 'CERNKEY') in subfields and not ('z', "%07d" % sysno) in subfields and not ('a', "%07d" % sysno) in subfields:
                print >> log, "WARNING: skipping record %s: different CERNKEY in 035 than the specified sysno (%s): %s" % (recid, sysno, subfields)
                continue
            if ('9', 'CDS') in subfields and not ('z', "%s" % cds_recid) in subfields and not ('a', "%s" % cds_recid):
                print >> log, "WARNING: skipping record %s: different CDS in 035 than the required cds recid (%s): %s" % (recid, cds_recid, subfields)
                continue

        cds_recid_already_there = False
        for position, value in record.iterfield("035__9"):
            if value == 'CERNKEY':
                position = position[:2] + (None, )
                record.delete_field(position)
            elif value == 'CDS':
                cds_recid_already_there = True

        for position, value in record.iterfield("8564_u"):
            if value.startswith("http://alice.cern.ch/format/showfull?sysnb="):
                position = position[:2] + (None, )
                record.delete_field(position)

        if not cds_recid_already_there:
            record.add_field("035__", value='', subfields=(('a', "%s" % cds_recid), ('9', 'CDS')))
        xml = record_xml_output(record)
        bibupload.add(xml)
        print >> tmp_output, xml
    except Exception, err:
        print >> log, "ERROR: Skipping record %s: %s" % (recid, err)
        print >> log, get_pretty_traceback()

bibupload.cleanup()

