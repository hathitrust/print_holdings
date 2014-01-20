#!/bin/bash

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null

data_dir="$SCRIPTPATH/../../data";
latest_hathi_file=`ls -w1 $data_dir | egrep '^hathi_full_[0-9]+.txt$' | sort | tail -1`;
ymd=`date +'%Y%m%d'`;
mhoff_outfile="htitem_oclc.$ymd.data";

python maketable_htitem_oclc_from_file.py $data_dir/$latest_hathi_file $data_dir/$mhoff_outfile;