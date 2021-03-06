#!/usr/bin/env python

import os
import sys
import difflib

from xml.dom.minidom import parse
# from lxml.doctestcompare import LXMLOutputChecker
# from doctest import Example
from xml.parsers.expat import ExpatError

def read_xml(path):
    def remove_unvalid_nodes(xml):
        for record in xml.childNodes:
            for datafield in record.getElementsByTagName('datafield'):
                if datafield.getAttribute('ind2') == '6':
                    for subfield in datafield.getElementsByTagName('subfield'):
                        if subfield.getAttribute('code') == 't' or subfield.getAttribute('code') == 'v':
                            datafield.removeChild(subfield)
        return xml

    try:
        xml = parse(path)
    except ExpatError:
        with open(path) as f:
            xml = f.read()
    else:
        xml = remove_unvalid_nodes(xml)
        xml = xml.toprettyxml(encoding='utf-8')
    return xml


def compare_results(results_dir, old_rev, new_rev, dest):
    for done, name in enumerate(sorted(os.listdir(os.path.join(results_dir, new_rev)))):
        if done % 100 == 99:
            print 'done', done + 1

        print 'processing', name

        old_xml = read_xml(os.path.join(results_dir, old_rev, name))
        new_xml = read_xml(os.path.join(results_dir, new_rev, name))

        if old_xml != new_xml:
            msg = 'Difference in %s' % name
            print msg
            print >>dest, msg
            for line in difflib.unified_diff(old_xml.split('\n'), new_xml.split('\n')):
                    print >>dest, line.strip('\n')
                    dest.flush()

        # sys.stdout.writelines(diff)
        # checker = LXMLOutputChecker()
        # if not checker.check_output(old_xml, new_xml, 0):
        #     message = checker.output_difference(Example("", old_xml), new_xml, 0)
        #     print 'Difference in %s' % name
        #     print message.encode('utf-8')


def usage():
    print >>sys.stderr, "refextract-diff.py old_rev new_rev"
    sys.exit(1)


def main():
    try:
        old_rev, new_rev = sys.argv[1:3]
    except ValueError:
        usage()

    print 'Comparing %s to %s' % (old_rev, new_rev)

    filename = "%s_to_%s.diff" % (old_rev, new_rev)
    dest_path = os.path.join(os.getenv('AFSHOME'), filename)
    results_dir = os.path.join(os.getenv('AFSHOME'), 'refextract-test-results')
    with open(dest_path, 'w') as dest:
        compare_results(results_dir, old_rev, new_rev, dest)

    print 'Results written to', dest_path

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print >>sys.stderr, "Interrupted"
