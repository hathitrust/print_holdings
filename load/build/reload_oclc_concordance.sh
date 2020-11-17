#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

# \d+_concordance.txt contains:
# ^<variant_ocn>\t<resolved_ocn>$
# And:
# $ grep -Pv '^\d+\t\d+$' concordance.txt
# ... should always give 0 results.

echo -n "DB username:";
read -s db_user;
echo "Gotcha.";

echo -n "DB password:";
read -s db_password;
echo "Gotcha.";

OCLCDIR="$DATADIR/oclc_concordance";

# Get the most recent concordance and unzip it.
echo "Getting the most recent OCLC concordance file from $OCLCDIR";
OCLC_FILE_GZ=`ls $OCLCDIR | grep -P '^\d+_concordance.txt.gz$' | sort -n | tail -1`;
OCLC_FILE="";

date;
echo "Extracting relevant records from $OCLCDIR/$OCLC_FILE_GZ into $OCLCDIR/concordance_1_ne_2.txt";
time zcat $OCLCDIR/$OCLC_FILE_GZ | awk -F'\t' '$1 != $2' > $OCLCDIR/concordance_1_ne_2.txt;

date;
echo "Setting up table";
time mysql -h $db_host -D $db_name -u $db_user -p$db_password < $OCLCDIR/oclc_concordance.sql;

date;
echo "Loading data";
time ruby load_oclc_concordance.rb;

date;
echo "DONE.";
