#!/bin/bash

for var in "$@";
do :
   member=`ls | grep -Po "^$var(\.estimate)?$"`;
   if [ -z "$member" ]; then
       echo "Could not find member dir for $var";
   else
       echo "Closing $member";
       tar --remove-files -cvzf $member.tar.gz $member/* && rmdir $member;
       echo "Done.";
   fi
done
	   
