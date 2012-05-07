import sys
from invenio.dbquery import run_sql


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


def db_saver(name, kb):
    for key, value in kb:
        run_sql('INSERT INTO knwKBRVAL (m_key,m_value,id_knwKB) VALUES (%s, %s, %s);', )

if __name__ == '__main__':
    for path in sys.argv[1:]:
        print 'Loading kb %s' % path
        load_kb_from_file(path, db_saver)