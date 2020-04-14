#!/bin/bash

member_id=$1
upload_file=$2

upload_path=$(egrep "^$member_id	" member_urls.txt | cut -f 2)

curl -n -T $upload_file "$upload_path"
