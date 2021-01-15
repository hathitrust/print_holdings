#!/bin/bash

# NOTE THAT THIS SCRIPT DOES NOT RUN "TIP HOUSE'S XLATE".
# So the OCLC resolution file will be the same as last build
# unless you make sure to update that before running this script.

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null

data_dir=`readlink -e $SCRIPTPATH/../../data`;
latest_hathi_file=`ls -w1 $data_dir/builds/current/ | egrep '^hathi_full(_[0-9]+)?.txt$' | sort | tail -1`;
mhoff_outfile="htitem_oclc.data";

echo "Started `date`";
echo "latest_hathi_file = ${latest_hathi_file}";

python $SCRIPTPATH/maketable_htitem_oclc_from_file.py $data_dir/builds/current/$latest_hathi_file $data_dir/builds/current/$mhoff_outfile;
exit_st=$?
if [ $exit_st != 0 ]; then
    echo "Exiting prematurely";
    exit $exit_st;
fi

ruby $SCRIPTPATH/reload_holdings_htitem_oclc.rb $data_dir/builds/current/$mhoff_outfile;
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

echo "Finished `date`";
echo "Made it all the way!";
