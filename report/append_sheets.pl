use strict;
use warnings;

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
use --op=<operator> to choose operator to compare cells with.
    Supported: - and %.
use --f=<fields> to specify which fields in the input sheet to use.
    <fields> is a comma-sep index list, like --f=1,3,5
    The first field in the list will be assumed to be member_id.
    Zero-indexed, unlike "cut -f". For the last field, do -1.

=cut

my $fi = 0;
# Separate input files from --flags, assign an index to each file and put in $files.
my $files = {map {++$fi => $_} grep {$_ !~ /^--/} @ARGV};

die "Need at least 2 infiles.\n" if keys %$files < 2;
my $lines      = {};
my $delim      = "\t";  # Value separator, change with --d=.
my $na         = 'N/A'; # The default for missing values. Change with --na.
my $header     = 0;     # Whether to use a header. Change with --header.
my $operator   = '';    # Choose operator to compare cells with, if any, with --op=
my $fields     = [];    # Specify which fields to take from sheet. Sort of like cut -f<fields>.
my $member_ids = {};
my $file_cols  = {};
my $number_rx  = qr/^[0-9]+(\.[0-9]+)?$/;

# Get/set all flags.
foreach my $m (grep {$_ =~ /^--/} @ARGV) {
    if ($m =~ /--na=(.+)/) {
	$na = $1;
    } elsif ($m =~ /^--d=(.+)/) {
	$delim = $1;
    } elsif ($m =~ /--header/) {
	$header = 1;
    } elsif ($m =~ /--op=(.+)/) {
	my $test_op = $1;
	if ($test_op eq '-') {
	    $operator = '-';
 	} elsif ($test_op eq '%') {
	    $operator = '%';
	}
    } elsif ($m =~ /--f=((-?[0-9]+,?)+)/) {
	$fields = [split(',', $1)];
    }
}

# For each file, read in its contents line per line.
foreach my $k (sort {$a<=>$b} keys %$files) {
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
	# If --f, extract desired cols.
	if (@$fields) {
	    @cols = @cols[@$fields];
	}
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
foreach my $k (sort {$a<=>$b} keys %$files) {
    # Check if there is a next file-hash to compare with
    my $h = $lines->{$files->{$k}};
    foreach my $m (sort {$a cmp $b} keys %$member_ids) {
	# Make sure each file has a row for each member in any row,
	# that has as many cols as any row in that file should.
	$h->{$m} ||= [($na) x $file_cols->{$k}];
	if (@{$h->{$m}} < $file_cols->{$k}) {
	    my $missing_vals = $file_cols->{$k} - @{$h->{$m}};
	    push(@{$h->{$m}}, $na) for (1 .. $missing_vals);
	}
	foreach my $cell (@{$h->{$m}}) {
	    push(@{$combined->{$m}}, $cell);
	}
    }
}

# Print the combined sheet using the same delimiter.
foreach my $m (sort keys %$combined) {
    if ($operator eq '-') {
	my @vals = @{$combined->{$m}};
	print $m . $delim . join(
	    $delim,
	    map {
		diff($vals[$_], $vals[$_+1])
	    } (0 .. @vals - 1)
	) . "\n";
    } elsif ($operator eq '%') {
	my @vals = @{$combined->{$m}};
	print $m . $delim . join(
	    $delim,
	    map {
		diff_percent($vals[$_], $vals[$_+1])
	    } (0 .. @vals - 1)
	) . "\n";
    } else {
	print $m . $delim . (join($delim, @{$combined->{$m}})) . "\n";
    }
}

sub diff {
    # Get the y-x diff for each 2 cells.
    my $x = shift;
    my $y = shift;
    return '' if !defined $y;
    if ($x =~ $number_rx && $y =~ $number_rx) {
	return $y - $x;
    } else {
	return $x;
    }
}

sub diff_percent {
    # Get the y-x diff% for each 2 cells.
    my $x = shift;
    my $y = shift;
    return '' if !defined $y;
    if ($x =~ $number_rx && $y =~ $number_rx) {
	return (100 * ($y - $x) / $x) . "%";
    } else {
	return $x;
    }
}
