# Potential replacement for /htapps/mwarin.babel/phdb_scripts/load/build/hathi_grabber.rb
# A bit more concise.

# Get the most recent hathi_full_YYYYMMDD.txt.gz listed in http://www.hathitrust.org/hathifiles
# and gunzip to local file.

list_url='https://www.hathitrust.org/hathifiles';
last_full_file_url=`curl ${list_url} | grep -Po '\"https:.+hathi_full_\d+\.txt\.gz\"' | sort -n | tail -1 | tr -d '"'`;
output_path=curled.txt;

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