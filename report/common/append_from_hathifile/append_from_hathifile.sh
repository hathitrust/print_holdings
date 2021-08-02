#!/bin/bash

# Purpose:
# Add information from a hathifile to a file with volume_ids.

# Invocation:
# bash append_from_hathifile.sh <inputfile> <hathifile> <fieldsfile> <@fields>

# Output written to <inputfile>.out

if [ $# -eq 0 ]; then
    # The perhaps most oblique "show help if no args given" I could manage.
    # Obviously sensitive to comment lines being added/removed.
    grep -m23 '^# ' $0
    exit 1
fi

# Arguments:
# <inputfile>:
# A .tsv file where the 1st col is volume_id.
# There can be any number of additional cols.
# 1 header line expected.
# E.g:
#
#   volume_id    col_2    col_3    ...    col_n
#   uc1.b4128429 val_x    val_y    ...    val_z
#   ...          ...      ...      ...    ...
inputfile=$1
shift

# <hathifile>:
# A full hathifile, e.g. hathi_full_20210301.txt from
# https://www.hathitrust.org/hathifiles
hathifile=$1
shift

# 0<fieldsfile>:
# A hathifields file, essentially the missing header line to $hathifile.
# hathi_field_list.txt from https://www.hathitrust.org/hathifiles.
fieldsfile=$1
shift

# <@fields>:
# The rest of the arguments, each must match a col in $fieldsfile,
# and be a field you want to copy from the hathifile to the output.
appendfields=$@

hathisort () {
    # Sort only by the first field (volume_id, which should be unique)
    # and use current dir for the temp-files sort makes,
    # gzip temp-files (and gzip -d when merging results)     
    sort -s -k1,1 -T ./ --compress-program=gzip
}

starttime=`date +'%Y-%m-%d %H:%M:%S'`

echo "### Run info:"
echo "# Started: $starttime"
echo "# input file: $inputfile"
echo "# hathifile: $hathifile"
echo "# fieldsfile: $fieldsfile"
echo "# appendfields: $appendfields"
echo "# trim $hathifile down to the fields: {$appendfields}, and append to $inputfile"

# Sort the input file, but keep header first.
# The sorting of the hathifile and inputfile must be the same.
echo "Sorting $inputfile to $inputfile.sort"
head -1 $inputfile > $inputfile.sort
egrep -v  '^volume_id' $inputfile | hathisort >> $inputfile.sort

# Get the column numbers for the given column names for cut -f
colnums=`ruby get_col_num_for_field_name.rb $fieldsfile $appendfields | grep colnums | cut -f2`
echo "# colnums: $colnums"

# Getting colnames in the order they appear (because cut -f requires it)
colnames=`ruby get_col_num_for_field_name.rb $fieldsfile $appendfields | grep colnames | cut -f2 | tr ',' '\t'`
echo "# colnames: $colnames"

# Trim and sort the hathifile. Always include 1st column (volume_id)
cut -f1,$colnums $hathifile | hathisort > hathifile.trim.sort

# Combine $inputfile.sort with hathifile.trim.sort, write to $inputfile.out
ruby append_from_hathifile.rb $inputfile.sort hathifile.trim.sort $colnames

# Remove temporary files.
rm hathifile.trim.sort
rm $inputfile.sort

stoptime=`date +'%Y-%m-%d %H:%M:%S'`
echo "# Stopped: $stoptime"
