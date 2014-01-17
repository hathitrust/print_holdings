# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

LOGPATH="$SCRIPTPATH/../../log";
CURRENTLOG="$LOGPATH/builds/current";

# Move all logfiles in the build (if any) to the current build log dir.
mv -v $SCRIPTPATH/*.log $CURRENTLOG;

todaystr=`date +'%Y-%m-%d'`;
FINISHEDLOG="$LOGPATH/builds/$todaystr";

# Rename the current build log dir.
mv -v $CURRENTLOG $FINISHEDLOG;

mkdir -p $CURRENTLOG;