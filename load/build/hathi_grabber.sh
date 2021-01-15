#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

# Replacement for build/hathi_grabber.rb, which is now placed in /experimental/.
# A bit more concise than the previous version.
# Get the most recent hathi_full_YYYYMMDD.txt.gz listed in https://www.hathitrust.org/hathifiles
# and gunzip to local file.

list_url='https://www.hathitrust.org/hathifiles';
last_full_file_url=`curl ${list_url} | tr -d "\n" | grep -Po '<a[^>]+>hathi_full_\d+.txt.gz</a>' | sort -n | tail -1 | egrep -o 'https:\/\/www.hathitrust.org\/filebrowser\/download\/[0-9]+'`;

output_path=$DATADIR/builds/current/hathi_full.txt;

if [ -z $last_full_file_url ]; then
    echo "Did not find a URL.";
    exit 1;
else
    echo "got [${last_full_file_url}]";
    curl ${last_full_file_url} | gunzip -c > ${output_path};
    if [ -s $ouput_path ]; then
	echo "$0 finished OK!";
	exit 0;
    else
	echo "${ouput_path} is empty. Fail!";
	exit 1;
    fi
fi
