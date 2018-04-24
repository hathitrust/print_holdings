#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

# concordance.txt contains:
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

# Only get the ones where one oclc maps to another oclc.
echo "Extracting relevant records from $OCLCDIR/concordance.txt into $OCLCDIR/concordance_1_ne_2.txt";
time cat $OCLCDIR/concordance.txt | awk -F'\t' '$1 != $2' > $OCLCDIR/concordance_1_ne_2.txt;

echo "Setting up table";
time mysql -h $db_host -D $db_name -u $db_user -p$db_password < $OCLCDIR/oclc_concordance.sql;

echo "Loading data";
ruby load_oclc_concordance.rb;
