#!/bin/bash

# NOTE THAT THIS SCRIPT DOES NOT RUN "TIP HOUSE'S XLATE".
# So the OCLC resolution file will be the same as last build
# unless you make sure to update that before running this script.

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null

data_dir=`readlink -e $SCRIPTPATH/../../data`;
latest_hathi_file=`ls -w1 $data_dir | egrep '^hathi_full_[0-9]+.txt$' | sort | tail -1`;
ymd=`date +'%Y%m%d'`;
mhoff_outfile="htitem_oclc.$ymd.data";

python $SCRIPTPATH/maketable_htitem_oclc_from_file.py $data_dir/$latest_hathi_file $data_dir/$mhoff_outfile;
exit_st=$?
if [ $exit_st != 0 ]; then
    echo "Exiting prematurely";
    exit $exit_st;
fi

ruby $SCRIPTPATH/reload_holdings_htitem_oclc.rb $data_dir/$mhoff_outfile;
exit_st=$?
if [ $exit_st != 0 ]; then
    echo "Exiting prematurely";
    exit $exit_st;
fi

ruby $SCRIPTPATH/generate_HT_AWS_file.rb;
exit_st=$?
if [ $exit_st != 0 ]; then
    echo "Exiting prematurely";
    exit $exit_st;
fi

# Keep an eye on this one.
ruby   -J-Xmx2042m $SCRIPTPATH/process_OCLC_resolution_index.rb;
exit_st=$?
if [ $exit_st != 0 ]; then
    echo "Exiting prematurely";
    exit $exit_st;
fi

echo "Made it all the way!";