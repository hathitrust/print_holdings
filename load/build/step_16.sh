#!/bin/bash

# Setup started ...

# Ensure we are on grog.
hng=`hostname | grep -o grog`;
if [ $hng == 'grog' ]; then
    echo "Running on grog.";
else
    echo "You need to run this on a host with access to the production MySQL database.";
    echo "(hint hint grog hint hint)";
    exit 1;
fi

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

# ... and setup finished.
date;

# Ran, took 2h 15 min.
echo "16b copy table in dev.";
ruby $SCRIPTPATH/populate_holdings_htitem_htmember_jn_dev.rb;
exit_st=$?                     
check_exit_code $exit_st;

# Ran, took 5 hours.
echo "16c, copy to production";
ruby $SCRIPTPATH/export_hhj_data.rb;
exit_st=$?                     
check_exit_code $exit_st;

echo "16d, generate delta files and load into db";
ruby -J-Xmx8000m $SCRIPTPATH/generate_updated_items_list.rb;
exit_st=$?                     
check_exit_code $exit_st;      

echo "Made it all the way!";
echo "Now run step_16e.rb manually."
exit 0;