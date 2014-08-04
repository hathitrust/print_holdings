#!/bin/bash

echo "Closing $1";
tar --remove-files -cvzf $1.tar.gz $1/* && rmdir $1;
echo "Done.";