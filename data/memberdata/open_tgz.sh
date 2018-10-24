#!/bin/bash

# if foo.estimate.tar.gz exists, then valid inputs are
# open_tgz.sh foo
# open_tgz.sh foo.estimate
# open_tgz.sh foo.estimate.tar.gz
# if foo.tar.gz exists, then valid inputs are
# open_tgz.sh foo
# open_tgz.sh foo.tar.gz


for var in "$@";
do :
   member=`ls | grep -Po "^$var(\.estimate)?(\.tar\.gz)?$" | sed -r 's/\.tar\.gz//'`;

   if [ -z "$member" ]; then
       echo "Could not find member dir for $member";
   else
       echo "Opening $member";
       tar -xvzf $member.tar.gz && rm $member.tar.gz;
       echo "Done.";
   fi
done
