#!/usr/bin/awk

# For when several members want to merge into one member.

# Expects an infile with holdings for the group, consisting of:
# volume_id \t actual_H \t group_H
# bc.ark:/13960/t03x8887b 12      1 
# Where we will recount the H for the group as actual_H - (group_h - 1)
# and then calculate the cost for the new Hs.

# Call thusly: awk -f recount.awk something.tsv
{
    new_h = $2 - ($3 - 1);
    new_hcounts[new_h]++;
    old_hcounts[$2]+= $3; # For comparison.
}

END {
    ic_cost = 0.21328463955220106;
    multi   = 1;

    for (h in new_hcounts) {
	h_cost       = (new_hcounts[h] / h) * ic_cost;
	h_cost_multi = h_cost * multi;
	h_cost_sum  += h_cost;
	print "new\t" h "\t" new_hcounts[h] "\t" h_cost "\t" h_cost_multi;
    } 
    print "New total cost:\t" h_cost_sum "\t" (h_cost_sum * multi)

    # This comparison is not fair. It does not take $3 into account.
    for (h in old_hcounts) {
    	h_cost       = (old_hcounts[h] / h) * ic_cost;
    	h_cost_multi = h_cost * multi;
    	old_h_cost_sum  += h_cost;
    	print "old\t" h "\t" old_hcounts[h] "\t" h_cost "\t" h_cost_multi;
    }
    print "Old total cost:\t" old_h_cost_sum "\t" (old_h_cost_sum * multi)
}
