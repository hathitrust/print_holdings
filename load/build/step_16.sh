#!/bin/bash

# Setup started ...

# Ensure we are on grog.
hng=`hostname | grep -o grog`;
if [ $hng == 'grog' ]; then
    echo "Running on grog.";
else
    echo "You need to run this on a host with access to the production MySQL database.";
    echo "(hint hint grog hint hint)";
    exit 0;
fi

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

# ... and setup finished.

bt="holdings_htitem_htmember_jn";

echo "16a: Back up table $bt";
bash $SCRIPTPATH/backuptable.sh $bt;

echo "16b copy table in dev.";
ruby $SCRIPTPATH/populate_holdings_htitem_htmember_jn_dev.rb;

echo "16c, copy to production";
ruby $SCRIPTPATH/export_hhj_data.rb;

echo "16d, generate delta files";
ruby -J-Xmx2048m $SCRIPTPATH/generate_updated_items_list.rb;