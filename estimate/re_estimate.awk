# If you estimate a member who is already a member, the H:s are off by one.
# So you decrease H and redistribute the cost.

/^[0-9]/ {
    if ($1 > 1) {
	tot_cost    = $1 * $3;
	h_min       = $1 - 1
	new_per_mem = tot_cost / h_min;
	run_tot    += new_per_mem;
	
	print h_min "\t" $2 "\t" new_per_mem "\t" run_tot;
    }
}
