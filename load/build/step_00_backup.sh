#!/bin/bash

# This is part of the backup step.
# Rather than making backups here and there during the process, let's
# do all the backups (that we can) before we start doing anything.

# This script can be nohupped.

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null

source $SCRIPTPATH/build_lib.sh;

mkdir -pv $BACKUPDIR/;
mkdir -pv $LOGDIR/;
date >> $LOGDIR/captain.log;
echo "Running step_00_backup.sh" >> $LOGDIR/captain.log;
mkdir -pv $DATADIR/builds/current/;

# Remember the line counts for input.
echo "Saving HT003 line counts.";
wc -l $HTDIR/HT003_*.tsv > $LOGDIR/ht00x_file_linecounts.txt

# holdings_htitem_htmember_jn -> holdings_htitem_htmember_jn_old, takes about 2,5 - 3 h.
echo "$SCRIPTPATH/make_hhhj_old.rb";
ruby $SCRIPTPATH/make_hhhj_old.rb;

echo "$0 done.";