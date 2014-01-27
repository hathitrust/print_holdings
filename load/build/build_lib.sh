#!/bin/bash

# Should be loaded by all .sh scripts in the build dir, thusly:

# pushd `dirname $0` > /dev/null
# SCRIPTPATH=`pwd`              
# popd > /dev/null              
# source $SCRIPTPATH/build_lib.sh;

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

DATADIR=`readlink -e $SCRIPTPATH/../../data`;
BACKUPDIR="$DATADIR/backup";
SERIALDIR="$DATADIR/serials";

LOGROOT=`readlink -e $SCRIPTPATH/../../log`;
LOGDIR="$LOGROOT/builds/current";

HTDIR=/htapps/pulintz.babel/data/phdb/HT003;

function check_exit_code() {
    exit_st=$1;
    if [ $exit_st != 0 ]; then     
	echo "Exiting prematurely";
	exit $exit_st;             
    fi                             
}

function echodo() {
    cmd=$1;
    echo "$cmd";
    $cmd;
}