#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

echo "Started `date`";

echo "6a started `date`";
jruby $SCRIPTPATH/assign_cluster_type.rb;
exit_st=$?
if [ $exit_st != 0 ]; then
    echo "Exiting prematurely";
    exit $exit_st;
fi
echo "6a finished `date`";

echo "6b started `date`";
jruby $SCRIPTPATH/num_items_cluster.rb;
exit_st=$?
if [ $exit_st != 0 ]; then
    echo "Exiting prematurely";
    exit $exit_st;
fi
echo "6b finished `date`";

echo "6c started `date`";
jruby $SCRIPTPATH/calc_cluster_rights.rb;
exit_st=$?
if [ $exit_st != 0 ]; then
    echo "Exiting prematurely";
    exit $exit_st;
fi
echo "6c finished `date`";

echo 'Done.';