#!/bin/bash

pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

d=`date +'%Y%m%d'`;

logfile="$LOGDIR/step_10e.log";

command="jruby $SCRIPTPATH/add_source_items_to_htitem_htmember_jn.rb $DATADIR/deposits.${d}.txt";
echo $command;
echo "nohup and logging to $logfile";
nohup $command > $logfile &
