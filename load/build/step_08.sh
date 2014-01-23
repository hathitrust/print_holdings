#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

# Combines all substeps of step 8 in the monthlies.

date=`date +%Y%m%d`;
datadir="/htapps/mwarin.babel/phdb_scripts/data";
chmd="${datadir}/cluster_htmember_multi.${date}.data";
hhhmd="${datadir}/holdings_htitem_htmember.multi.${date}.data";

cmd1="jruby /htapps/mwarin.babel/phdb_scripts/load/build/relabel_mpm.rb";
cmd2="jruby -J-Xmx2000m /htapps/pulintz.babel/Code/phdb/bin/multipart_cluster_mapper.rb $chmd";
cmd3="jruby /htapps/pulintz.babel/Code/phdb/bin/generate_htitem_htmember_jn_data.rb $chmd $hhhmd";

echo `date`; 
echo "$cmd1"; 
$cmd1;

echo `date`; 
echo "$cmd2"
$cmd2;

echo `date`; 
echo "$cmd3"; 
$cmd3;