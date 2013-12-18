#!/bin/awk

# Takes an input file that looks like:
#
# #items  cc      ac
# 1388379 1       0
#  821498 2       0
#  590645 3       0
#  466398 4       0
#  368995 5       0
#
# Call thusly:
# awk -f crystallize_liquid.awk cic_liquid.tsv | sort -nr > cic_crystal.tsv

# Count 2 things per line and store in separate buckets with $2 as key:
# 1. Sum of $1
# 2. Sum of ($1 * $3) 

NR <= 1 {next} # Skip column headers
{
    print $2 "\t" $1 "\t" $3; 
    a[$2] += $1; 
    b[$2] += ($1 * $3)
} 
END {
    for (i in a) 
	print i "_sum\t" b[i] "_open\t" (a[i] - b[i]) "_closed"
}

# Output needs to be sorted to make sense.