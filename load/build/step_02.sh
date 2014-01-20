#!/bin/bash

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null
DATAPATH="$SCRIPTPATH/../../data";

data_dir="$SCRIPTPATH/../../data";
latest_hathi_file=`ls -w1 $data_dir | egrep '^hathi_full_[0-9]+.txt$' | sort | tail -1`;

python maketable_htitem_oclc_from_file.py $data_dir/$latest_hathi_file;