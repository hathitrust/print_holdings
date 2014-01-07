#!/bin/bash

# Takes a file of patterns as 1st arg and a list of files as 2-nth arg(s).
# Greps for each pattern in all the files.

basefile=$1;
if [ ! -f $basefile ]; then
    echo "Require a valid file path as 1st arg. [$basefile] is not that.";
    exit;
fi

shift;

hitsfile="${basefile}.hits";

if [ $# -gt 0 ]; then

    if [ -f $hitsfile ]; then
	rm $hitsfile;
    fi
    echo "Printing results to $hitsfile";

    cat $basefile | while read line 
      do :
	echo $line;
	grep ^${line}\$ $* >> $hitsfile;
    done
else
    echo "Need some files as 2nd-nth arg(s).";
    exit;
fi



