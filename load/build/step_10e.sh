#!/bin/bash

pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

jruby $SCRIPTPATH/add_source_items_to_htitem_htmember_jn.rb $DATADIR/builds/current/deposits.txt;
