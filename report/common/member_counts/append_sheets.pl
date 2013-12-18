use strict;
use warnings;

my $fi = 0;
my $files = {map {++$fi => $_} @ARGV};
die "Need at least 2 infiles.\n" if keys %$files < 2;
my $lines = {};
my $member_ids = {};
my $file_cols  = {};
my $delim = "\t";
my $na    = 'N/A';

# Take any number of tsv files greater than or equal to 2.
# For each row in file x, add all cols to a corresponding row
# in a combined sheet.
# Assuming that all rows in a file has the same number of cols.
# Number of cols may be different between files.
# Also assuming an identifier in the first col, in all files,
# which is not constantly reappended to the combined sheet.

# So 2 files like:
#
# a 1 3 5
# and
# a 8 9
# b 5 5
#
# ... will result in:
#
# a 1   3   5   8 9
# b N/A N/A N/A 5 5

# For each file, read in its contents line per line.
foreach my $k (sort keys %$files) {
    open(F, $files->{$k});
    # Store lines here, with the first cell value as key (assuming no dups)
    my $lines_in_file = {};
    while (<F>) {
	my $line = $_;
	chomp $line;
	my @cols = split($delim, $line);
	# Save first cell value for hash key.
	my $member_id = shift @cols;
	# The other cells to array ref, store as hash value.
	$lines_in_file->{$member_id} = [@cols];
	$member_ids->{$member_id}    = 1;
	if (!defined $file_cols->{$k} || @cols > $file_cols->{$k}) {
	    $file_cols->{$k} = @cols;
	}
    }
    # Store entire hash in file hash.
    $lines->{$files->{$k}} = $lines_in_file;
    close(F);
}

my $combined = {map {$_ => []} keys %$member_ids};

# For each file in the file hash:
foreach my $k (sort keys %$files) {
    # Check if there is a next file-hash to compare with
    my $h = $lines->{$files->{$k}};
    foreach my $m (sort {$a cmp $b} keys %$member_ids) {
	# Make sure each file has a row for each member in any row,
	# that has as many cols as any row in that file should.
	$h->{$m} ||= [($na) x $file_cols->{$k}];
	if (@{$h->{$m}} < $file_cols->{$k}) {
	    my $diff = $file_cols->{$k} - @{$h->{$m}};
	    push(@{$h->{$m}}, $na) for (1 .. $diff);
	}
	foreach my $cell (@{$h->{$m}}) {
	    push(@{$combined->{$m}}, $cell);
	}
    }
}

# Print the combined sheet using the same delimiter.
foreach my $m (sort keys %$combined) {
    print $m . $delim . (join($delim, @{$combined->{$m}})) . "\n";
}
