# Takes an arbitrary ruby command (presumably one that requires db auth)
# and does the db auth prompt before nohupping the ruby command.
#
# Example:
#
# $ bash db_auth_nohup.sh ruby lala.rb
# DB username:***
# DB password:***
# Running: nohup ruby lala.rb > db_auth_nohup_2020_05_13_13_09_33.log &

# Getting date for the log:
date_str=`date +'%Y%m%d_%H%M%S'`;
log_fn="db_auth_nohup_$date_str.log";

# Logging to the master log.
echo -e "$log_fn\t$*" >> db_auth_nohup_master.log;

# Getting db auth.
echo -n "DB username:";
read -s dbu;
echo "Gotcha.";

echo -n "DB password:";
read -s dbp;
echo "Gotcha.";

# Run the thing.
echo "Running: nohup $* > $log_fn &";
echo $* > $log_fn;
nohup env CMDLINE_ENV_DB_USER="$dbu" CMDLINE_ENV_DB_PASSWORD="$dbp" $* >> $log_fn &
