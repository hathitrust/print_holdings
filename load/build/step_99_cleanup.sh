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