#!/bin/awk
# Takes a file of mysql output as input.
# Counts the occurrence of volume_id and whether it has access.

NR <= 1 {next} # Skip first line with column headers.
{
    if ($3 == "allow") {
	vol_o[$1] += 1;
    } else {
	vol_c[$1] += 1;
    }
} 
END {
    print "cc\tac\tvolume_id";
    for (i in vol_o) print vol_o[i] "\t1\t" i;
    for (i in vol_c) print vol_c[i] "\t0\t" i;
}