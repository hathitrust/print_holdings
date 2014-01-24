#!/bin/bash

# Combines scriptable post-manual substeps of step 9 (the cloud step) in the monthlies.

pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

date=`date +%Y%m%d`;
datadir=`readlink -e $SCRIPTPATH/../../data`;
awsdir=$datadir/aws/$date;

date;
echo "Started";
mkdir -pv $awsdir;

s3cmd get s3://umich-lib-phdb/output/$date/* $awsdir/;

ruby $SCRIPTPATH/load_awsdata.rb start $awsdir/;

date;
echo "Finished.";
