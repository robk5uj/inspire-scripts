import sys
import urllib
import traceback
import ElementTree as ET
from job_helper import ChunkedBibUpload, ChunkedBibIndex
from invenio.docextract_record import get_record, BibRecord
from invenio.search_engine import perform_request_search

SCRIPT_NAME = 'add_doi'
WRITE_LOG = True
WRITE_ERRORS = True
LOG_DIRECTORY = './'

# initialize messages arrays and logging functions
errors = []
messages = []


def enable_log(log_name):
    def log_message(message):
        if message == 'CLOSE':
            log.close()
            return
        log.write(message + '\n')
    log = open(LOG_DIRECTORY + log_name, 'w')
    return log_message


def append_doi(recID, doi):
    record = get_record(recid=recID)
    try:
        # make sure that there is no DOI for this record
        if record.find_subfields('0247_a'):
            messages.append('Record %s already has a doi' % recID)
            if record.find_subfields('0247_a')[0].value != doi:
                errors.append('DOI of %s record is different than the new doi (%s)!'
                              % (recID, doi))
        else:
            # create new record with only 0247 field, that we will append
            # to the existing record with bibupload function
            new_record = BibRecord(recID)
            new_field = new_record.add_field('0247_')
            new_field.add_subfield('a', doi.decode('utf-8'))
            new_field.add_subfield('2', 'DOI')

            messages.append('Successfully inserted the doi: ' + doi +
                            ' to the record ' + str(recID))

            return new_record.to_xml()
    except Exception, e:
        traceback.print_exc()
        errors.append('Unknown error: ' + repr(e))


def add_doi(arxiv):
    # if arxiv doesn't contain "arXiv:" at the beginning, we add it
    if arxiv[:6] != 'arXiv:':
        arxiv = 'arXiv:' + arxiv
    # messages.append("Processing the " + arxiv + " arXiv number")
    return perform_request_search(p='037__a:' + arxiv)


def main():
    bibupload = ChunkedBibUpload(mode='a', user=SCRIPT_NAME, notimechange=True)
    bibindex = ChunkedBibIndex(indexes='reportnumber', user=SCRIPT_NAME)

    # open url and parse xml
    source = sys.argv[1]
    tree = ET.parse(urllib.urlopen(source))
    root = tree.getroot()

    for item in root.iter('article'):
        doi = item.get('doi')
        arxiv = item.get('preprint_id')
        recID = add_doi(arxiv)
        if recID:
            recID = recID[0]
            record_xml = append_doi(recID, doi)
            if record_xml:
                messages.append("Now we will run the bibupload and bibindex for " + str(recID) + " record")
                messages.append("We will upload the following xml code " + repr(record_xml))
                bibupload.add(record_xml)
                bibindex.add(recID)
    if WRITE_ERRORS:
        er = enable_log('errors.txt')
        map(er, errors)
        er("CLOSE")
    if WRITE_LOG:
        msg = enable_log('log.txt')
        map(msg, messages)
        msg("CLOSE")

    bibupload.__del__()
    bibindex.__del__()


if __name__ == '__main__':
    main()
