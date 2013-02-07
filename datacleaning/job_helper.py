import os
import time

from tempfile import mkstemp

from invenio.dbquery import run_sql
from invenio.config import CFG_TMPDIR
from invenio.bibtask import task_low_level_submission


def submit_bibupload_task(to_submit, mode, user, priority=3, notimechange=False):
    # Save new record to file
    (temp_fd, temp_path) = mkstemp(prefix='copy-doi',
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

        self.submitter(self, self.to_submit, *self.args, **self.kwargs)

    def add(self, el):
        self.to_submit.append(el)
        if len(self.to_submit) == self.chunk_size:
            self.submit_task()

    def __del__(self):
        if self.to_submit:
            self.submit_task()


class ChunkedBibUpload(ChunkedTask):
    submitter = submit_bibupload_task


class ChunkedBibIndex(ChunkedTask):
    submitter = submit_bibupload_task


def all_recids(recids):
    max_id = run_sql("SELECT max(id) FROM bibrec")[0][0]
    return xrange(1, max_id + 1)


def loop(recids, callback):
    for done, recid in enumerate(recids):
        if done % 50 == 0:
            print 'done %s of %s' % (done + 1, len(recids))
        callback(recid)
