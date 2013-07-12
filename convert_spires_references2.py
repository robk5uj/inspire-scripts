#!/usr/bin/env python

import sys

ARXIV_MARKER = 'arXiv:'
ARXIV_ID_LEN = 9

def convert_references_file(path):
    print 'converting', path
    f = open(path, 'r')
    try:
        convert_references(f)
    finally:
        f.close()


def convert_references(text):
    for counter, ref in enumerate(text):
        ref = ref.strip().strip(';%<> ')
        # Journal ?
        if ARXIV_MARKER in ref:
            code = 'r'
            start_index = ref.find(ARXIV_MARKER)
            ref = ref[start_index:start_index+len(ARXIV_MARKER)+ARXIV_ID_LEN]
        elif ',' in ref:
            code = 's'
        else:
            code = 'r'

        print """<datafield tag="999" ind1="C" ind2="5">
    <subfield code="o">%(counter)s</subfield>
    <subfield code="%(code)s">%(ref)s</subfield>
</datafield>""" % {'counter': counter + 1, 'code': code, 'ref': ref}


if __name__ == '__main__':
    files = sys.argv[1:]
    if not files:
        print 'Usage: python convert_references.py <filename1> [<filename2>]'
    for path in files:
        try:
            convert_references_file(path)
        except IOError:
            print >>sys.stderr, 'File unreadable', path
