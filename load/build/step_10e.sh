d=`date +'%Y%m%d'`;

pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

datadir=`readlink -e $SCRIPTPATH/../../data`;
logdir=`readlink -e $SCRIPTPATH/../../log`;
logfile="$logdir/step_10e.log";

command="jruby $SCRIPTPATH/add_source_items_to_htitem_htmember_jn.rb $datadir/deposits.${d}.txt";
echo $command;
echo "nohup and logging to $logfile";
nohup $command > $logfile &
