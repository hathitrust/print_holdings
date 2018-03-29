#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

# First run the thing that cannot be nohupped (requires user/passwd):
bash step_00_backup_interactive.sh;

nohuppable=$(cat <<EOF
    bash   step_00_backup.sh &&
    ruby   write_loadfile.rb &&
    ruby   load_selected.rb  &&
    bash   step_01.sh        &&
    bash   step_02.sh        &&
    python step_03.py        &&
    ruby   step_04.rb        &&
    ruby   step_05.rb        &&
    bash   step_06.sh        &&
    bash   step_08.sh        &&
    bash   step_09a.sh       ;
EOF
)

# Echo quoted preserves the newlines from the heredoc.
echo "$nohuppable";
echo "$nohuppable" > runthis.sh;
nohup bash runthis.sh >> build.log &

# Now do the Amazon EMR if there were any changes.
# When that's done, do batch_build_02.sh
