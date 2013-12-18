#!/bin/awk

# Takes an input file that looks like:
#
# 13      161     3
# 12      104     3
# 3       94      2
# 2       65      2
# 11      54      3

# Call thusly:
# awk -f crystallize_liquid.awk cic_liquid.tsv | sort -nr > cic_crystal.tsv

# Count 2 things per line and store in separate buckets with $2 as key:
# 1. Sum of $1
# 2. Sum of ($1 * $3) 
{
    print $2 "\t" $1 "\t" $3; 
    a[$2] += $1; 
    b[$2] += ($3 * $1)
} 
END {
    for (i in a) 
	print i "_sum\t" (a[i] - b[i]) "_open\t" b[i] "_closed"
}

# Output needs to be sorted to make sense.