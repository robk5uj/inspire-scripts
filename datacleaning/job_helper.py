import os
import time

from tempfile import mkstemp
from operator import itemgetter
# from xml.etree import ElementTree as ET
import ElementTree as ET

from invenio.dbquery import run_sql
from invenio.config import CFG_TMPDIR
from invenio.bibtask import task_low_level_submission
from invenio.search_engine import get_record as get_record_original

def submit_bibupload_task(to_submit, mode, user, priority=3, notimechange=False):
    # Save new record to file
    (temp_fd, temp_path) = mkstemp(prefix=user,
                                   dir=CFG_TMPDIR)
    temp_file = os.fdopen(temp_fd, 'w')
    temp_file.write('<?xml version="1.0" encoding="UTF-8"?>')
    temp_file.write('<collection>')
    for el in to_submit:
        temp_file.write(el)
    temp_file.write('</collection>')
    temp_file.close()

    args = ['bibupload', user,
            '-P', str(priority), '-%s' % mode,
            temp_path]
    if notimechange:
        args += ['--notimechange']

    return task_low_level_submission(*args)


def wait_for_task(task_id):
    sql = 'select status from schTASK where id = %s'
    while run_sql(sql, [task_id])[0][0] != 'DONE':
        time.sleep(5)


def submit_bibindex_task(to_update, indexes, user, priority=3):
    recids = [str(r) for r in to_update]
    return task_low_level_submission('bibindex', user,
                                     '-w', indexes,
                                     '-P', str(priority),
                                     '-i', ','.join(recids))


class ChunkedTask(object):
    """Groups elements in chunks before submitting them to bibsched"""
    chunk_size = 500
    submitter = None

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.to_submit = []

    def submit_task(self, *args, **kwargs):
        if self.submitter is None:
            raise Exception('Task submitter not defined')

        task_id = self.submitter(self.to_submit, *self.args, **self.kwargs)
        wait_for_task(task_id)

    def add(self, el):
        self.to_submit.append(el)
        if len(self.to_submit) == self.chunk_size:
            self.submit_task()
            self.to_submit = []

    def __del__(self):
        if self.to_submit:
            self.submit_task()


class ChunkedBibUpload(ChunkedTask):
    submitter = staticmethod(submit_bibupload_task)


class ChunkedBibIndex(ChunkedTask):
    submitter = staticmethod(submit_bibindex_task)


def all_recids():
    max_id = run_sql("SELECT max(id) FROM bibrec")[0][0]
    return xrange(1, max_id + 1)


def loop(recids, callback):
    for done, recid in enumerate(recids):
        callback(recid)
        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))


def get_record(recid):
    def create_control_field(inst):
        return BibRecordControlField(inst[3].decode('utf-8'))

    def create_field(inst):
        subfields = [BibRecordSubField(code, value.decode('utf-8')) \
                                                for code, value in inst[0]]
        return BibRecordField(ind1=inst[1], ind2=inst[2], subfields=subfields)

    record = BibRecord()
    for tag, instances in get_record_original(recid).iteritems():
        if tag.startswith('00'):
            record[tag] = [create_control_field(inst) for inst in instances]
        else:
            record[tag] = [create_field(inst) for inst in instances]

    return record


class BibRecord(object):
    def __init__(self, recid=None):
        self.record = {}
        if recid:
            self.record['001'] = [BibRecordControlField(str(recid))]

    def __setitem__(self, tag, fields):
        self.record[tag] = fields

    def __getitem__(self, tag):
        return self.record[tag]

    def __eq__(self, b):
        if set(self.record.keys()) != set(b.record.keys()):
            return False

        for tag, fields in self.record.iteritems():
            if set(fields) != set(b[tag]):
                return False

        return True

    def __hash__(self):
        return hash(tuple(self.record.iteritems()))

    def __repr__(self):
        if '001' in self.record:
            s = u'BibRecord(%s)' % list(self['001'])[0].value
        else:
            s = u'BibRecord()'
        return s

    def to_xml(self):
        root = ET.Element('record')
        for tag, fields in sorted(self.record.iteritems(), key=itemgetter(0)):
            for field in fields:
                if tag.startswith('00'):
                    controlfield = ET.SubElement(root,
                                                 'controlfield',
                                                 {'tag': tag})
                    controlfield.text = field.value
                else:
                    attribs = {'tag': tag,
                               'ind1': field.ind1,
                               'ind2': field.ind2}
                    datafield = ET.SubElement(root, 'datafield', attribs)
                    for subfield in field.subfields:
                        attrs = {'code': subfield.code}
                        s = ET.SubElement(datafield, 'subfield', attrs)
                        s.text = subfield.value
        return ET.tostring(root)


class BibRecordControlField(object):
    def __init__(self, value):
        self.value = value

    def __eq__(self, b):
        return self.value == b.value

    def __hash__(self):
        return hash(self.value)


class BibRecordField(object):
    def __init__(self, ind1=" ", ind2=" ", subfields=None):
        self.ind1 = ind1
        self.ind2 = ind2
        if subfields is None:
            subfields = []
        self.subfields = subfields

    def __repr__(self):
        return 'BibRecordField(ind1="%s",ind2="%s", subfields=%s)' % (self.ind1, self.ind2, self.subfields)

    def __eq__(self, b):
        return self.ind1 == b.ind1 and self.ind2 == b.ind2 \
                    and set(self.subfields) == set(b.subfields)

    def __hash__(self):
        return hash((self.ind1, self.ind2, tuple(self.subfields)))


class BibRecordSubField(object):
    def __init__(self, code, value):
        self.code = code
        self.value = value

    def __eq__(self, b):
        return self.code == b.code and self.value == b.value

    def __hash__(self):
        return hash((self.code, self.value))
