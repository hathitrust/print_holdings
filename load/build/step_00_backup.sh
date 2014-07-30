#!/bin/bash

# This is the backup step.
# Rather than making backups here and there during the process, let's
# do all the backups (that we can) before we start doing anything.

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null

source $SCRIPTPATH/build_lib.sh;

mkdir -pv $BACKUPDIR/;
mkdir -pv $LOGDIR/;
mkdir -pv $LOGDIR/builds/current/;
mkdir -pv $DATADIR/builds/current/;

# Remember the line counts for input.
echo "Saving HT003 line counts.";
wc -l $HTDIR/HT003_*.tsv > $LOGDIR/ht00x_file_linecounts.txt

# holdings_htitem_htmember_jn -> holdings_htitem_htmember_jn_old, takes about 2,5 - 3 h.
echo "$SCRIPTPATH/make_hhhj_old.rb";
ruby $SCRIPTPATH/make_hhhj_old.rb;

# Back up holdings_htitem_oclc to file.
bash $SCRIPTPATH/backup_table.sh holdings_htitem_oclc;

# Used to be 12b.
bash $SCRIPTPATH/backup_table.sh holdings_H_counts

# Back up holdings_htitem_htmember_jn to file.
echo "$SCRIPTPATH/backup_table.sh";
bash $SCRIPTPATH/backup_table.sh holdings_htitem_htmember_jn;

echo "$0 done.";