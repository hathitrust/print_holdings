#!/bin/bash
# Part of step_00_backup.sh
# Makes a gzipped backup of a given table in host mysql-htprep schema ht_repository.
# Table 'foo' will be saved as "$BK_LOCATION/foo_YMD.sql.gz".

BK_LOCATION=/htapps/mwarin.babel/phdb_scripts/data/sql;

if [ $1 ]; then
    tablename=$1;
    todaydate=`date +'%Y-%m-%d'`;
    backupfile="backup_${tablename}_${todaydate}.sql.gz";
    user=`whoami`;
    host='mysql-htprep';
    schema='ht_repository';

    # You will be prompted for password at this point.
    command="mysqldump --quick --dump-date -h $host -u $user -p $schema $tablename";
    echo "$command > $BK_LOCATION/$backupfile";
    $command | gzip > $BK_LOCATION/$backupfile;
else
    echo "Need tablename as arg.";
fi