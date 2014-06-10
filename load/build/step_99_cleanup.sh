#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

ruby $SCRIPTPATH/trunc_old.rb;

# Move all logfiles in the build (if any) to the current build log dir.
mv -v $SCRIPTPATH/*.log $LOGDIR;

todaystr=`date +'%Y-%m-%d'`;

# Rename the current build log dir.
mv -v $LOGDIR $LOGROOT/builds/$todaystr;

# Recreate $LOGROOT/builds/current
mkdir -p $LOGDIR;

# Targz some bulky files left over:
## AWS:
last_aws=`ls $DATADIR/aws/ | tail -1`
echo "Making a tar.gz of AWS $last_aws"
## cd to the dir instead of messing with --strip-components.
cd $DATADIR/aws/$last_aws/;
tar --remove-files -czvf $last_aws.tar.gz part-*;
cd $SCRIPTPATH;
