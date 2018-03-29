#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

# Getting username and password at the beginning,
# and passing through to step_10b so it doesn't have to ask for them and break nohup.
# Search CMDLINE_ENV_DB_ in Hathidb for how that works.

echo -n "DB username:";
read -s db_user;
echo "Gotcha.";

echo -n "DB password:";
read -s db_password;
echo "Gotcha.";

nohuppable=$(cat <<EOF
    bash step_09c.sh &&
    env CMDLINE_ENV_DB_USER="$db_user" CMDLINE_ENV_DB_PASSWORD="$db_password" ruby step_10b.rb &&
    ruby step_10c.rb &&
    ruby step_10d.rb &&
    ruby step_10e_new.rb &&
    ruby step_10f.rb &&
    ruby step_10g.rb &&
    ruby step_10h.rb &&
    ruby step_11.rb  &&
    ruby step_12.rb   ;
EOF
)

# Echo quoted preserves the newlines from the heredoc.
echo "$nohuppable";
echo "$nohuppable" > runthis.sh;

nohup bash runthis.sh >> build.log &

# Now run reports, and finally go to prod and run steps 16, 16e and update build date.
