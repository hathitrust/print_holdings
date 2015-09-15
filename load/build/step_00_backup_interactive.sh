#!/bin/bash

# This is part of the backup step.
# This part cannot be nohupped.

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null

source $SCRIPTPATH/build_lib.sh;

date >> $LOGDIR/captain.log;
echo "Running step_00_backup_interactive.sh" >> $LOGDIR/captain.log;

echo "vvv Going to need password here! vvv";

# Back up holdings_htitem_oclc to file.
bash $SCRIPTPATH/backup_table.sh holdings_htitem_oclc;

# Used to be 12b.
bash $SCRIPTPATH/backup_table.sh holdings_H_counts

# Back up holdings_htitem_htmember_jn to file.
bash $SCRIPTPATH/backup_table.sh holdings_htitem_htmember_jn;

echo "$0 done.";