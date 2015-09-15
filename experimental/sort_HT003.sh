#!/bin/bash

# Take an infile, make an outfile with .out suffix.
# Take the first line of infile and put it in outfile.
# Bar the first line in the infile, sort the rest of the
# lines numerically and put them in outfile.
# Now, if the file size is the same for infile and outfile,
# overwrite infile with outfile.

# The files in this case are prepared for mysql LOAD DATA INFILE
# purposes, and supposedly it is much faster if the data is sorted.
# The column headers is on the first line, that is why it needs 
# a little extra attention. 

function sort_ht_file {
    infl=$1;
    echo "sorting $infl";
    out_file=$infl.out;
    head -1 $infl > $out_file;
    tail -n+2 $infl | sort -n >> $out_file;

    infl_size=`stat -c '%s' $infl`;
    out_file_size=`stat -c '%s' $out_file`;

    if [ $infl_size == $out_file_size ]
    then
	mv $out_file $infl;
    else
	echo 'Error';
	rm $outfile;
    fi
}

if [ -f $1 ]; then
    sort_ht_file $1;
elif [ -d $1 ]; then
    for f in $1/HT003*.tsv
    do
	sort_ht_file $f;
    done
fi