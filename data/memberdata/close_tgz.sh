#!/bin/bash

member=`ls | grep -Po "^$1(\.estimate)?$"`;

if [ -z "$member" ]; then
    echo "Could not find member dir for $1";
else
    echo "Closing $member";
    tar --remove-files -cvzf $member.tar.gz $member/* && rmdir $member;
    echo "Done.";
fi

