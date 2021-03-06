#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

source $SCRIPTPATH/build_lib.sh;

local_hathi_file=$1; # pass in if you have a file locally that you want to use
latest_hathi_file="";

if [ -z $local_hathi_file ]; then
    # Gets the "latest_hathi_file" from www.
    bash $SCRIPTPATH/hathi_grabber.sh;
    exit_st=$?;
    check_exit_code $exit_st;
    latest_hathi_file=`ls -w1 $DATADIR/builds/current/ | egrep '^hathi_full.txt$' | sort | tail -1`;
    echo "latest_hathi_file = ${latest_hathi_file}";
else
    echo "using local hathifile = ${local_hathi_file}"
    latest_hathi_file=$local_hathi_file
fi

# Assuming this was manually placed here.
latest_serial_file=`ls -w1 $SERIALDIR | grep -v README | tail -1`;
echo "latest_serial_file = ${latest_serial_file}";

# Generate a hathi_full_YYYYMMDD.data file
echo "ruby ${SCRIPTPATH}/maketable_htitem_from_file.rb ${latest_hathi_file} serials/${latest_serial_file};"
ruby $SCRIPTPATH/maketable_htitem_from_file.rb $latest_hathi_file serials/$latest_serial_file;
exit_st=$?;
check_exit_code $exit_st;

# Reload holdings_htitem with the output from the previous script.
ruby $SCRIPTPATH/reload_holdings_htitem.rb;
exit_st=$?;
check_exit_code $exit_st;

echo "Finished with an OK exit code ($exit_st)";

