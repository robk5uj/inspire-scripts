import re
import sys

from invenio.bibknowledge import add_kb_mapping

re_kb_line = re.compile(ur'^\s*(?P<seek>[^\s]+)\s+(?P<repl>[^\s].*)\s*$',
                        re.UNICODE)


def load_kb_from_file(path, builder):
    try:
        fh = open(path, "r")
    except IOError, e:
        raise StandardError("Unable to open kb '%s': %s" % (path, e))

    def lazy_parser(fh):
        for rawline in fh:
            if rawline.startswith('#'):
                continue

            try:
                rawline = rawline.decode("utf-8").rstrip("\n")
            except UnicodeError:
                raise StandardError("Unicode problems in kb %s at line %s" \
                                                             % (path, rawline))

            # Test line to ensure that it is a correctly formatted
            # knowledge base line:
            # Extract the seek->replace terms from this KB line
            m_kb_line = re_kb_line.search(rawline)
            if m_kb_line:  # good KB line
                yield m_kb_line.group('seek'), m_kb_line.group('repl')
            else:
                raise StandardError("Badly formatted kb '%s' at line %s" \
                                                            % (path, rawline))

    try:
        return builder(lazy_parser(fh))
    finally:
        fh.close()


def db_saver(kb_name, kb):
    f = open('out.kb', 'w')
    for key, value in kb:
        f.write(' \t%s\t%s\n' % (key.strip(), value.strip()))
    f.close()


def usage():
    print >> sys.stderr, "Usage load-into-kb.py kb_name file.kb"


if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage()
        sys.exit(1)
    kb_name = sys.argv[1]

    for path in sys.argv[2:]:
        print 'Loading kb %s' % path
        load_kb_from_file(path, lambda kb: db_saver(kb_name, kb))
