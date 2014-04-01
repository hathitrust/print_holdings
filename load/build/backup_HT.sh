#!/bin/bash

# tar -zcvf archive-name.tar.gz directory-name
# Where,
# -z: Compress archive using gzip program
# -c: Create archive
# -v: Verbose i.e display progress while creating archive
# -f: Archive File name

ht_dir='/htapps/mwarin.babel/phdb_scripts/data/loadfiles';
backup_dir='/htapps/mwarin.babel/phdb_scripts/data/backup/loadfiles';
dstring=`date +'%Y-%m-%d'`;
fname="backup_HT003_${dstring}.tar.gz";
command="tar -zcvf $backup_dir/$fname $ht_dir"

echo "tar -zcvf $backup_dir/$fname $ht_dir";
$command;