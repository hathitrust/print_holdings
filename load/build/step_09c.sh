#!/bin/bash

# Combines scriptable post-manual substeps of step 9 (the cloud step) in the monthlies.

pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

# Get most recent output bucket.
date=`s3cmd ls s3://$s3_main_bucket/output/ | grep -Po '\/(\d+)\/$' | tr -d '/' | sort | tail -1`;
awsdir=$DATADIR/aws/$date;

date;
echo "Started $0";
echo "awsdir = ${awsdir}";
mkdir -pv $awsdir;

# Check if data has already been downloaded:
if [ -f $awsdir/_SUCCESS ]; then
    echo "AWS data already downloaded?";
    ls -l $awsdir;
    # Check if there is a .tar.gz with data there already.
    compressed=`ls $awsdir | grep -Po '[\d-]+.tar.gz' | sort | tail -1`;
    if [ ! -z "$compressed" ]; then
	echo "Found compressed data, $compressed. Extracting:";
	cd $awsdir;
	tar -xzvf $compressed && rm $compressed;
	cd -;
	# step_99 will compress these again when the build is over.
    fi
else
    # No existing data here, go get it.
    echo "GETting files from s3://$s3_main_bucket/output/$date/ to $awsdir/";
    s3cmd get s3://$s3_main_bucket/output/$date/* $awsdir/;
    # Check success.
    if [ -f $awsdir/_SUCCESS ]; then
	echo "Looks like the MapReduce was a success.";
    else
	echo "Looks like the MapReduce was a fail. Exiting.";
	exit 1;
    fi
fi

date;
echo "Loading files into DB.";
ruby $SCRIPTPATH/load_awsdata.rb start $awsdir/;

date;
echo "Finished $0";
