#!/bin/bash
# Part of step_00_backup.sh
# Makes a gzipped backup of a given table in host mysql-htprep schema ht_repository.
# Table 'foo' will be saved as "$BK_LOCATION/foo_YMD.sql.gz".

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

BK_LOCATION=$DATADIR/sql;

if [ $1 ]; then
    tablename=$1;
    todaydate=`date +'%Y-%m-%d'`;
    backupfile="backup_${tablename}_${todaydate}.sql.gz";
    user=`whoami`;

    # You will be prompted for password at this point.
    command="mysqldump --quick --dump-date -h $db_host -u $user -p $db_name $tablename";
    echo "$command > $BK_LOCATION/$backupfile";
    $command | gzip > $BK_LOCATION/$backupfile;
    echo "Wrote $BK_LOCATION/$backupfile";
else
    echo "Need tablename as arg.";
fi
