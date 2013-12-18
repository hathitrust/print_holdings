d=`date +'%Y%m%d'`;
command="jruby /htapps/pulintz.babel/Code/phdb/bin/add_source_items_to_htitem_htmember_jn.rb /htapps/mwarin.babel/phdb_scripts/data/deposits.${d}.txt";
echo $command;
$command;