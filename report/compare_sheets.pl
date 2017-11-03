use strict;
use warnings;

my $fi = 0;
my $files = {map {++$fi => $_} @ARGV};
my $lines = {};
my $rec_sep = "\t";
my $number_rx = qr/^[0-9]+(\.[0-9]+)?$/;

# Take any number of files greater than or equal to 2.
# Compare the cells in the nth file with the n+1th.
if (keys %$files < 2) {
    die "Need at least 2 infiles.\n";
}

# For each file, read in its contents line per line.
foreach my $k (sort keys %$files) {
    open(F, $files->{$k});
    # Store lines here, with the first cell value as key (assuming no dups)
    my $lines_in_file = {};
    while (<F>) {
	my $line = $_;
	chomp $line;
	my @cols = split($rec_sep, $line);
	# Save first cell value for hash key.
	my $member_id = shift @cols;
	# The other cells to array ref, store as hash value.
	$lines_in_file->{$member_id} = [@cols];
    }
    # Store entire hash in file hash.
    $lines->{$files->{$k}} = $lines_in_file;
    close(F);
}

# For each file in the file hash:
foreach my $k (sort keys %$files) {
    my $kplus = $k + 1;
    # Check if there is a next file-hash to compare with
    if (defined $files->{$kplus}) {
	print "#### DIFF $k ==> $kplus ####\n";
	my ($h1, $h2) = ($lines->{$files->{$k}}, $lines->{$files->{$kplus}});
	# Get all member_ids from both hashes.
	my %all_member_ids = map {$_ => 1} (keys %$h1, keys %$h2);
	foreach my $m (sort {$a cmp $b} keys %all_member_ids) {
	    # Create null-row if member_id is missing in either hash.
	    $h1->{$m} ||= [map {0} @{$h2->{$m}}];
	    $h2->{$m} ||= [map {0} @{$h1->{$m}}];
	    print "$m\t";
	    # Compare each cell in h1 against corresponding cell in h2.
	    print join(
		"\t",
		map {
		    compare_cells($h2->{$m}->[$_], $h1->{$m}->[$_]);
		} (0 .. @{$h1->{$m}} - 1)
	    ) . "\n";
	}
    }
}

sub compare_cells {
    my $x = shift;
    my $y = shift;
    if ($x =~ $number_rx && $y =~ $number_rx) {
	return $x - $y;
    } else {
	return $x;
    }
}
