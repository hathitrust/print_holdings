#!/bin/bash

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null

data_dir=`readlink -e $SCRIPTPATH/../../data`;
latest_hathi_file=`ls -w1 $data_dir | egrep '^hathi_full_[0-9]+.txt$' | sort | tail -1`;
ymd=`date +'%Y%m%d'`;
mhoff_outfile="htitem_oclc.$ymd.data";

python $SCRIPTPATH/maketable_htitem_oclc_from_file.py $data_dir/$latest_hathi_file $data_dir/$mhoff_outfile;

ruby   $SCRIPTPATH/reload_holdings_htitem_oclc.rb $data_dir/$mhoff_outfile;

ruby   $SCRIPTPATH/generate_HT_AWS_file.rb;

# Keep an eye on this one.
ruby   -J-Xmx2042m $SCRIPTPATH/process_OCLC_resolution_index.rb;

# ruby $SCRIPTPATH/step_02e_queries.rb;