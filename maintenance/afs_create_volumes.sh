#!/bin/bash

if [ $# -eq 0 ]
  then
    echo "No arguments supplied. You need to give folder prefix (11 for g110..g119 etc.)."
    exit 1
fi

DATA_FOLDER=/afs/cern.ch/project/inspire/PROD/var/data/files/

cd $DATA_FOLDER

for i in 0 1 2 3 4 5 6 7 8 9; do afs_admin create_volume p.inspire.g$1$i; done
for i in 0 1 2 3 4 5 6 7 8 9; do afs_admin create_mount g$1$i p.inspire.g$1$i; done
for i in 0 1 2 3 4 5 6 7 8 9; do afs_admin sa g$1$i _inspire_ rlidwka; done
for i in 0 1 2 3 4 5 6 7 8 9; do afs_admin set_quota g$1$i 10G; done
