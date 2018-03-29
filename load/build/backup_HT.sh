#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

source $SCRIPTPATH/build_lib.sh;

# tar -zcvf archive-name.tar.gz directory-name
# Where,
# -z: Compress archive using gzip program
# -c: Create archive
# -v: Verbose i.e display progress while creating archive
# -f: Archive File name

dstring=`date +'%Y-%m-%d'`;
fname="backup_HT003_${dstring}.tar.gz";
command="tar -zcvf $BACKUPDIR/loadfiles/$fname $HTDIR"

echo $command;
$command;
