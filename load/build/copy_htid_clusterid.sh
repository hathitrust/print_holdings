echo -n "DB username:";
read -s db_user;
echo "Gotcha.";

echo -n "DB password:";
read -s db_password;
echo "Gotcha.";

nohup env CMDLINE_ENV_DB_USER="$db_user" CMDLINE_ENV_DB_PASSWORD="$db_password" ruby copy_htid_clusterid.rb > copy_htid_clusterid.log &
