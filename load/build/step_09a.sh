#!/bin/bash

# Combines scriptable substeps of step 9 (the cloud step) in the monthlies.

pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

date=`date +%Y%m%d`;
datadir=`readlink -e $SCRIPTPATH/../../data`;

input_current="s3://umich-lib-phdb-1/input/Current";
ht003_dir="/htapps/pulintz.babel/data/phdb/HT003";

echo "Before:";
s3cmd ls $input_current/;

# Empty the current input bucket.
date;
echo "DELeting old data.";
s3cmd del $input_current/*;

# Put current HT003 files in current input bucket.
date;
echo "PUTting new data.";
s3cmd put $ht003_dir/HT003_*.tsv $input_current/;

echo "After:";
s3cmd ls $input_current/;