#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

source $SCRIPTPATH/build_lib.sh;

# Combines all substeps of step 8 in the monthlies.

date=`date +%Y%m%d`;
chmd="${DATADIR}/cluster_htmember_multi.${date}.data";
hhhmd="${DATADIR}/holdings_htitem_htmember.multi.${date}.data";

# Fairly quick.
cmd1="jruby $SCRIPTPATH/relabel_mpm.rb";
# Takes ~9h.
cmd2="jruby -J-Xmx2000m $SCRIPTPATH/multipart_cluster_mapper.rb $chmd";
# Takes a couple of  minutes.
cmd3="jruby $SCRIPTPATH/generate_htitem_htmember_jn_data.rb $chmd $hhhmd";

echo `date`;
echo "$cmd1";
$cmd1;
exit_st=$?;
check_exit_code $exit_st;

echo `date`;
echo "$cmd2";
$cmd2;
exit_st=$?;
check_exit_code $exit_st;

echo "Sanity-checking lines in $hhhmd:";
wc -l $hhhmd;

echo "Sanity-checking cluster_id 590 in $hhhmd:";
grep -r "^590\s" $hhhmd;

echo `date`;
echo "$cmd3";
$cmd3;
exit_st=$?;
check_exit_code $exit_st;

echo `date`;
echo 'Finished';
