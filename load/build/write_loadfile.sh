#!/bin/bash

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null

source $SCRIPTPATH/build_lib.sh;

outf=$DATADIR/builds/current/load.tsv
echo "Writing to ${outf}";

cd $DATADIR/memberdata; 
perl ~/useful/tree.pl | grep HT003 | egrep -v '\.estimate|old' | sed -r 's/.+(HT003_(.+).(mono|multi|serial).tsv)/\2\t\3/g' | sort > $outf;
cd -;