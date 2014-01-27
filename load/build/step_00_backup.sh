#!/bin/bash

# This is the backup step.
# Rather than making backups here and there during the process, let's
# do all the backups (that we can) before we start doing anything.

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null
DATAPATH="$SCRIPTPATH/../../data";
LOGPATH="$SCRIPTPATH/../../log/builds/current";
BACKUPPATH="$DATAPATH/backup";

HT_DIR=/htapps/pulintz.babel/data/phdb/HT003;
mkdir -pv $BACKUPPATH/;

# Back up holdings_htitem_htmember_jn to file.
echo "$SCRIPTPATH/backup_table.sh";
bash $SCRIPTPATH/backup_table.sh holdings_htitem_htmember_jn;

# holdings_htitem_htmember_jn -> holdings_htitem_htmember_jn_old
echo "$SCRIPTPATH/make_hhhj_old.rb";
ruby $SCRIPTPATH/make_hhhj_old.rb;

# Back up holdings_htitem_oclc to file.
bash $SCRIPTPATH/backup_table.sh holdings_htitem_oclc;

# Used to be 12b.
bash $SCRIPTPATH/backup_table.sh holdings_H_counts

# Remember the line counts for input.
wc -l $HT_DIR/HT003_*.tsv > $LOGPATH/ht00x_file_linecounts.txt