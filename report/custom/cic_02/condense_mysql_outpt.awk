#!/bin/awk
# Takes a file of mysql output as input.
# Expecting:
#
# oclc    member_id       cc      ac
# 2       chi             2       0
# 2       ind             2       0
# 2       iowa            4       0
# 2       minn            2       0
# 2       nwu             2       0
#
# ... where the first line will be skipped.
#
# Counts the occurrence of oclc and how many of them have access.

NR <= 1 {next} # Skip first line with column headers.
{
    a[$1] += 1;               # Increment for each institution that has the oclc.
    b[$1] += ($4 > 0 ? 1 : 0) # Save count of access_counts (any access counts as +1)
} 
END {
    # How many has it, how many allow access, and what is its oclc.
    print "cc\tac\toclc";
    for (i in a) 
	print a[i] "\t" b[i] "\t" i;
}