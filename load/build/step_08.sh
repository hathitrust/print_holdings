#!/bin/bash
# Combines all substeps of step 8 in the monthlies.

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

chmd="${DATADIR}/builds/current/cluster_htmember_multi.data";
hhhmd="${DATADIR}/builds/current/holdings_htitem_htmember.multi.data";

# Fairly quick.
cmd1="jruby $SCRIPTPATH/relabel_mpm.rb";
# Takes ~9h.
cmd2="jruby -J-Xmx4000m $SCRIPTPATH/multipart_cluster_mapper.rb $chmd";
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

echo "Sanity-checking lines in $chmd:";
wc -l $chmd;

echo "Sanity-checking cluster_id 590 in $chmd:";
grep -r "^590\s" $chmd;

echo `date`;
echo "$cmd3";
$cmd3;
exit_st=$?;
check_exit_code $exit_st;

echo `date`;
echo 'Finished';
