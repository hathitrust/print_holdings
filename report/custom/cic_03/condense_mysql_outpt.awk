#!/bin/awk
# Takes a file of mysql output as input.
# Expecting:
#
# volume_id member_id cc ac
# 2         chi       2  0
# 2         ind       2  0
# 2         iowa      4  0
# 2         minn      2  0
# 2         nwu       2  0
#
# ... where the first line will be skipped.
#
# Counts the occurrence of volume_id and how many of them have access.

NR <= 1 {next} # Skip first line with column headers.
{
    a[$1] += 1;               # Increment for each institution that has the volume_id.
    b[$1] += ($4 > 0 ? 1 : 0) # Save count of access_counts (any access counts as +1)
} 
END {
    # How many has it, how many allow access, and what is its volume_id.
    print "cc\tac\tvolume_id";
    for (i in a) 
	print a[i] "\t" b[i] "\t" i;
}