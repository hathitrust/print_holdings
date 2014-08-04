#!/bin/bash

echo "Opening $1";
tar -xvzf $1.tar.gz && rm $1.tar.gz;
echo "Done.";