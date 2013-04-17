import os
import sys
import getopt
import time
import tempfile

from invenio.invenio_connector import InvenioConnector

""" Small script used to generate BibUpload MARCXML of Springer fulltexts """


def create_MARC(recid, paths):
    out = []
    out.append('\t<record>')
    out.append('\t\t<controlfield tag="001">' + str(recid) + '</controlfield>')
    for path in paths:
        out.append('\t\t<datafield tag="FFT" ind1=" " ind2=" ">\n' +
                   '\t\t\t<subfield code="a">' + path.strip() + '</subfield>\n' +
                   '\t\t\t<subfield code="t">Springer</subfield>\n' +
                   '\t\t\t<subfield code="r">Springer</subfield>\n' +
                   '\t\t\t<subfield code="o">HIDDEN</subfield>\n' +
                   '\t\t</datafield>')
    out.append('\t</record>')
    return "\n".join(out)


def find_files(dirpath):
    found_files = []
    found_sizes = []
    for possible_file in os.listdir(dirpath):
        #if possible_file.endswith('.pdf') or possible_file.endswith('.xml'):
        if possible_file.endswith('.xml') and os.path.getsize(os.path.join(dirpath, possible_file)) not in found_sizes:
            found_files.append(possible_file)
            found_sizes.append(os.path.getsize(os.path.join(dirpath, possible_file)))
    return found_files


def main():
    usage = """
    Usage:
    $ springer_pdf_import /path/to/fulltexts/dir/ > upload.xml

    Specify the path to the folder containing Springer fulltexts.

    The folder is expected to contain sub-folders with the DOI as the
    folder name and it contains .xml or .pdf files to upload for
    any matched record.

    FOLDER NAME
    10.1007_s12648-012-0203-2/

    ACTUAL DOI
    10.1007/s12648-012-0203-2

    Matches against INSPIRE DB and produces the following MARCXML
    output per file.

    <record>
        <controlfield tag="001">RECID</controlfield>
        <datafield tag="FFT" ind1=" " ind2=" ">
            <subfield code="a">PATH_TO_FULLTEXT_FILE</subfield>
            <subfield code="t">Springer</subfield>
            <subfield code="r">Springer</subfield>
            <subfield code="o">HIDDEN</subfield>
        </datafield>
    </record>

    Outputs to stdout.
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "u:hx", [])
    except getopt.GetoptError, e:
        sys.stderr.write("Error:" + e + "\n See help below.\n")
        print usage
        sys.exit(1)

    if len(args) < 1:
        sys.stderr.write("Error: You need to specify a directory.\n")
        sys.exit(1)

    records_path = args[0]
    file_mode = False

    for opt, opt_value in opts:
        if opt in ["-h"]:
            print usage
            sys.exit(0)
        if opt in ["-u"]:
            server_url = opt_value.strip()
        if opt in ["-f"]:
            file_mode = True

    if file_mode:
        fulltext_dirs = open(records_path).readlist()
    else:
        fulltext_dirs = os.listdir(records_path)
    missing = []
    server_url = "http://inspirehep.net"
    ambig = []
    out = []
    out.append("<collection>")
    count = 0
    for dirpath in fulltext_dirs:
        count += 1
        if file_mode:
            # dirpath is a absolute path. Break it down!
            full_name = os.path.splitext(dirpath)[0]
            dirpath = os.path.basename(full_name)
        doi = dirpath.replace('_', '/')
        sys.stderr.write("Getting %s ... \n" % (doi,))
        try:
            server = InvenioConnector(server_url)
            recid = server.search(p="doi:%s" % (doi,), of="id")
        except Exception, e:
            sys.stderr.write("Error: %s\n" % (str(e),))
            count = 100
            recid = ""
        sys.stderr.write("Result: %s \n" % (str(recid),))
        if not recid:
            missing.append(dirpath)
        elif len(recid) > 1:
            ambig.append("%s %s %s" % (dirpath, doi, str(recid)))
        else:
            filepaths = []
            if not file_mode:
                # Find files
                filepaths = [os.path.join(records_path, dirpath, path)
                             for path in find_files(os.path.join(records_path, dirpath))]
            else:
                filepaths.append(dirpath)
            out.append(create_MARC(recid[0], filepaths))
        if count >= 100:
            time.sleep(10.0)
            count = 0
    out.append("</collection>")

    fd, dummy = tempfile.mkstemp(".txt", "%s_missing_records" % (os.path.basename(records_path),), "./")
    os.write(fd, "\n".join(missing))
    os.close(fd)

    fd, dummy = tempfile.mkstemp(".txt", "%s_ambig_records" % (os.path.basename(records_path),), "./")
    os.write(fd, "\n".join(ambig))
    os.close(fd)

    print "\n".join(out)

if __name__ == '__main__':
    main()
