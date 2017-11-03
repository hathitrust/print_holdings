use strict;
use warnings;

my $fi = 0;
my $files = {map {++$fi => $_} grep {$_ !~ /^--/} @ARGV};
die "Need at least 2 infiles.\n" if keys %$files < 2;
my $lines = {};
my $delim = "\t";  # Value separator, change with --d=.
my $na    = 'N/A'; # The default for missing values. Change with --na.
my $header = 0;    # Whether to use a header. Change with --header.
my $member_ids = {};
my $file_cols  = {};

=pod

Take any number of tsv files greater than or equal to 2.
For each row in file x, add all cols to a corresponding row
in a combined sheet.
Assuming that all rows in a file has the same number of cols.
Number of cols may be different between files.
Also assuming an identifier in the first col, in all files,
which is not constantly reappended to the combined sheet.

So 2 files like:

a 1 3 5
and
a 8 9
b 5 5

... will result in:

a 1   3   5   8 9
b N/A N/A N/A 5 5

use --na=<value> to override the default value of $na ('N/A').
use --d=<value> to override the default value of $delim ("\t").
use --header to sneak in the filenames in the appended sheet.

=cut

foreach my $m (grep {$_ =~ /^--/} @ARGV) {
    if ($m =~ /--na=(.+)/) {
	$na = $1;
    } elsif ($m =~ /^--d=(.+)/) {
	$delim = $1;
    } elsif ($m =~ /--header/) {
	$header = 1;
    }
}

# For each file, read in its contents line per line.
foreach my $k (sort keys %$files) {
    open(F, $files->{$k});
    # Store lines here, with the first cell value as key (assuming no dups)
    my $lines_in_file = {};
    if ($header) {
	# If --header set, fake a member_id called _header with the file as val.
	# This way you can see which value came from which file.
	$lines_in_file->{'_header'} = [$files->{$k}];
	$member_ids->{'_header'} = 1;
    }
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
