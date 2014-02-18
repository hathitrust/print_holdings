#!/bin/bash

# Combines scriptable pre-manual substeps of step 9 (the cloud step) in the monthlies.

pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

input_current="s3://umich-lib-phdb-1/input/Current";

date;
echo "Before:";
s3cmd ls $input_current/;

# Empty the current input bucket.
date;
echo "DELeting old data.";
s3cmd del $input_current/*;

# Put current HT003 files in current input bucket.
date;
echo "PUTting new data.";
s3cmd --no-progress put $HTDIR/HT003_*.tsv $input_current/;

echo "After:";
s3cmd ls $input_current/;

date;
echo "Finished.";