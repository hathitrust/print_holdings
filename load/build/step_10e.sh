#!/bin/bash

pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

logfile="$LOGDIR/builds/current/step_10e.log";

command="jruby $SCRIPTPATH/add_source_items_to_htitem_htmember_jn.rb $DATADIR/builds/current/deposits.txt";
echo $command;
echo "nohup and logging to $logfile";
nohup $command > $logfile &
