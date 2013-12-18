#!/bin/awk
# Takes a file of mysql output as input.
# Expecting 
#
# +-----------------------------+-----------+-------+------+
# | volume_id                   | member_id | cc    | ac   |
# +-----------------------------+-----------+-------+------+
# | bc.ark:/13960/t0000n50w     | chi       |     1 |    0 |
#
# ... where the first 3 lines will be skipped.
#
# Counts the occurrence of volume_ids and how many of them have access.
#
# Call thusly:
# awk -f condense_mysql_outpt.awk > cic_liquid.tsv

NR <= 3 {next} # Skip lines
{
    # print $2 "\t" $8;       # These are what counts.
    a[$2] += 1;               # Save count of unique volume_id
    b[$2] += ($8 > 0 ? 1 : 0) # Save count of access_counts (any access counts as +1)
} 
END {
    # How many has it, how many allow access, and what is its volume_id.
    for (i in a) 
	print a[i] "\t" b[i] "\t" i;
}