#!/bin/bash

# Combines scriptable post-manual substeps of step 9 (the cloud step) in the monthlies.

pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

date=`date +%Y%m%d`;
awsdir=$DATADIR/aws/$date;

date;
echo "Started";
mkdir -pv $awsdir;

echo "GETting files into $awsdir/";
s3cmd get s3://umich-lib-phdb/output/$date/* $awsdir/;

if [ -f $awsdir/_SUCCESS ]; then
    echo "Looks like the MapReduce was a success.";
else
    echo "Looks like the MapReduce was a fail. Exiting.";
    exit(1);
fi

date;
echo "Loading files into DB.";
ruby $SCRIPTPATH/load_awsdata.rb start $awsdir/;

date;
echo "Finished.";
