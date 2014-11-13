#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

ruby $SCRIPTPATH/trunc_old.rb;

## Move all logfiles in the build (if any) to the current build log dir.
mv -v $SCRIPTPATH/*.log $LOGDIR;

todaystr=`date +'%Y-%m-%d'`;

## Rename the current build log & data dirs.
mv -v $LOGDIR $LOGROOT/builds/$todaystr;
mv -v $DATADIR/builds/current $DATADIR/builds/$todaystr;

## Recreate $LOGROOT/builds/current
mkdir -p $LOGDIR;
mkdir -p $DATADIR/builds/current;

## Targz some bulky files left over:
## AWS:
last_aws=`ls -d1 $DATADIR/aws/*/ | tail -1`
echo "Making a tar.gz of AWS $last_aws"
## cd to the dir instead of messing with --strip-components.
cd $last_aws/;
tar --remove-files -czvf $todaystr.tar.gz part-*;
cd $SCRIPTPATH;
