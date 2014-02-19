import logging
import ConfigParser
from invenio.config import CFG_ETCDIR

logging.getLogger().setLevel(level=logging.DEBUG)

def load_config(key='citation'):
    config_path = CFG_ETCDIR + "/bibrank/" + key + ".cfg"
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_path))
    return config

config = load_config()

from invenio.bibrank_citation_indexer import process_chunk


print "process_chunk([1191918], config)"

r = process_chunk([344808], config)

print repr(r)
