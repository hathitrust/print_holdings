d=`date +'%Y%m%d'`;
logfile="/htapps/mwarin.babel/phdb_scripts/load/build/step_10e.log";
command="jruby /htapps/mwarin.babel/phdb_scripts/load/build/add_source_items_to_htitem_htmember_jn.rb /htapps/mwarin.babel/phdb_scripts/data/deposits.${d}.txt";
echo $command;
echo "nohup and logging to $logfile"
nohup $command > $logfile &
