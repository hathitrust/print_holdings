#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

# Gets the "latest_hathi_file".
ruby $SCRIPTPATH/hathi_grabber.rb;
exit_st=$?
if [ $exit_st != 0 ]; then
    echo "Exiting prematurely";
    exit $exit_st;
fi

data_dir="$SCRIPTPATH/../../data";
latest_hathi_file=`ls -w1 $data_dir | egrep '^hathi_full_[0-9]+.txt$' | sort | tail -1`;

# Assuming this was manually placed here.
serial_dir="$data_dir/serials";
latest_serial_file=`ls -w1 $serial_dir | tail -1`;

# Generate a hathi_full_YYYYMMDD.data file
ruby $SCRIPTPATH/maketable_htitem_from_file.rb $latest_hathi_file serials/$latest_serial_file;

exit_st=$?
if [ $exit_st != 0 ]; then
    echo "Exiting prematurely";
    exit $exit_st;
fi

# Reload holdings_htitem with the output from the previous script.
ruby reload_holdings_htitem.rb;

exit_st=$?
if [ $exit_st == 0 ]; then
    echo "Finished with an OK exit code ($exit_st)";
fi
