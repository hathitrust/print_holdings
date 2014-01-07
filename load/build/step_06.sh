#!/bin/bash

thisdir='/htapps/mwarin.babel/phdb_scripts/load/build';

echo "Started `date`";

echo "6a started `date`";
jruby $thisdir/assign_cluster_type.rb;
echo "6a finished `date`";

echo "6b started `date`";
jruby $thisdir/num_items_cluster.rb;
echo "6b finished `date`";

echo "6c started `date`";
jruby $thisdir/calc_cluster_rights.rb;
echo "6c finished `date`";

echo 'Done.';