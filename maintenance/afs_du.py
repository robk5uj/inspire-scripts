#!/usr/bin/env python

import re
import subprocess
import os
import sys
from pprint import pprint
from cStringIO import StringIO

# '/afs/cern.ch/project/inspire/uploads' is a mount point for volume '#p.inspire.uploads'
_RE_LSMOUNT = re.compile("is a mount point for volume '#(.*?)'$")


def get_volume_for_path(path):
    """
    Returns the corresponding AFS volume for which the given path is a
    mountpoint. Return "" in case the path is not a mount point.
    """
    output, stderr = subprocess.Popen(
        ['fs', 'lsmount', path], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    if stderr:
        return ""
    return _RE_LSMOUNT.search(output).group(1)


def find_volume_for_path(path):
    """
    Search for the AFS volume where the given path is stored.
    """
    if os.path.join(os.path.sep, 'afs', 'cern.ch') in path:
        volume = get_volume_for_path(path)
        if volume:
            return volume
        else:
            return find_volume_for_path(os.path.dirname(path))
    else:
        raise ValueError("Cannot find suitable AFS volume")


def crawl_path(path):
    """
    Crawl the given path, staying in the current AFS volume.
    Reports about the top ten most populated directories (i.e. those
    with the most cumulated long file names) and return the top 40 most filled
    directories.
    """
    path = os.path.realpath(path)
    if path.endswith(os.path.sep):
        path = path[:-len(os.path.sep)]
    current_volume = find_volume_for_path(path)
    print "Staying in volume: %s" % current_volume

    namespace_crowd = []
    size_summary = {}

    for root, dirs, files in os.walk(path):
        print "Analysing %s" % root
        for dirname in list(dirs):
            fulldir = os.path.join(root, dirname)
            volume = get_volume_for_path(fulldir)
            if volume and volume != current_volume:
                print "Skipping %s as it is mounted on a different volume: %s" % (fulldir, volume)
                dirs.remove(dirname)
        namespace_crowd.append((len(''.join(files)), len(files), root))
        for filename in files:
            fullpath = os.path.join(root, filename)
            size = os.path.getsize(fullpath)
            current_path = root
            while len(current_path) >= len(path):
                if current_path in size_summary:
                    size_summary[current_path] += size
                else:
                    size_summary[current_path] = size
                current_path = os.path.dirname(current_path)
    namespace_crowd.sort()
    print "Most crowded directories:"
    pprint(namespace_crowd[-10:])
    size_summary = size_summary.items()
    size_summary = sorted(size_summary, key=lambda x: x[1])
    print "Hugest directories:"
    pprint(size_summary[-40:])

if __name__ == "__main__":
    crawl_path(os.path.realpath(sys.argv[1]))
