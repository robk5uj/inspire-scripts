#!/usr/bin/env python

import os
import time
import sys


def prune_path(path, age=2 * 30 * 24 * 60 * 60, dryrun=True, verbose=False):
    """
    Remove files older than age in any directory under path.
    If a directory is empty it is also removed.
    """
    import os
    import time
    today = time.time()
    total_size = 0
    total_deleted = 0
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            fullname = os.path.join(root, name)
            file_stat = os.stat(fullname)
            total_size += file_stat.st_size
            if today - file_stat.st_mtime > age:
                total_deleted += file_stat.st_size
                if not dryrun:
                    os.remove(fullname)
                if verbose:
                    print "file %s -> REMOVED (freed %s out of %s: %s%%)" % (fullname, total_deleted, total_size, total_deleted * 100 / total_size)
            else:
                if verbose:
                    print "file %s -> PRESERVED" % fullname
        for name in dirs:
            fullname = os.path.join(root, name)
            if not os.listdir(fullname):
                if not dryrun:
                    os.rmdir(fullname)
                if verbose:
                    print "directory %s -> REMOVED" % fullname
            else:
                if verbose:
                    print "directory %s -> PRESERVED" % fullname

if __name__ == "__main__":
    prune_path(
        sys.argv[1], dryrun='--yes-i-know' not in sys.argv, verbose=True)
