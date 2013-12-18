#!/bin/bash

# Get the rawest data from the database, a list of oclc, member_id, copy_count and access_count.
# Assumes that you the user has read-permissions for schema ht_repository in db mysql-htdev. 

user=`whoami`;
infile='get_all_cic.sql';
outfile='step_01_out.tsv';

mysql -h mysql-htdev -u $user -p -D ht_repository < $infile > $outfile;