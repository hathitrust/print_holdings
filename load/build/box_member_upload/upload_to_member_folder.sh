#!/bin/bash

# Usage:
# bash upload_to_member_folder.sh <member_id> <file> <remote_subfolder>

# Example:
# bash upload_to_member_folder.sh umich monos.tsv "print holdings"
# ... will upload the file monos.tsv to the subfolder "print holdings" in the umich box.

member_id=$1
upload_file=$2
subfolder=$3

if [[ ! $subfolder =~ ^(print holdings|shared print|analysis|test)$ ]]
then
    echo "param 3 must match ^(print holdings|shared print|analysis)$"
    echo "\"$subfolder\" does not"
fi

upload_path=$(grep -P "^$member_id\t" member_urls.txt | cut -f 2)
echo "Uploading $upload_file to $upload_path$subfolder/"
curl -n -T $upload_file "$upload_path$subfolder/"
