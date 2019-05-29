use strict;
use warnings;

=pod

Normalization script originally written to handle whitman data. Martin Warin / HathiTrust 2016.
Take a line like

  "ocm10754151,(OCoLC)0010754151,(OCoLC)ocm10754151 "

and turn it into

  (OCoLC)ocm10754151

If there are several surface forms that fundamentally have the same number part, then only one such surface form is printed.
If there are actual different numbers in one line, one of each will be printed. I.e., turn:

  "(OCoLC)820123724,(OCoLC)ocn820123724,(OCoLC)828626853,(OCoLC)ocn828626853,(OCoLC)828626853"

into

  (OCoLC)828626853,(OCoLC)ocn820123724

Alter default behavior with:

  --column_delim
    Give a string to use as column delimiter

  --data_delim
    Give a string to use as data delimiter between multiple ocns

  --oclc_column=\d
    Give the (zero-indexed) column number where ocn is expected

  --verbose=\d
    Give 1 to turn on verbose output

  --header=\d
    Give a number for number of lines at the top of the file to skip

  --strict=\d
    Give 0 to turn off strict ocn checking. Under strict, bare numbers
    such as 555 are not considered ocns. Strict ocns must match /oc.+(\d+)/i

=cut

my $default = {
    column_delim  => "\t",
    data_delim    => ",",
    oclc_column   => 0,
    verbose       => 0,
    header        => 0,
    strict        => 0,
    semi_strict   => 0,
};

# Look for default overrides in @ARGV and set accordingly.
foreach my $k (keys %$default) {
    my ($input) = map {/^--$k=(.+)/ ? $1 : ()} @ARGV;
    $default->{$k} = $input if $input;
    print STDERR "## $k:[$default->{$k}]\n";
}

# Filter out --option_name from ARGV.
@ARGV = grep {$_ !~ /^--/} @ARGV;

while (<>) {
    my $line = $_;
    chomp $line;

    # Fast-forward past header.
    if ($default->{header} > 0) {
	$default->{header}--;
	print $line . "\n";
	print STDERR "Skipping header [$line]\n";
	next;
    }

    my $old_line = $line;
    $line =~ s/\"//g;
    # Split line into columns
    my @columns = split($default->{column_delim}, $line);
    if (!defined $columns[$default->{oclc_column}]) {
	print STDERR "No oclc_column ($default->{oclc_column}) in line [$line]\n" if $default->{verbose};
	next;
    }

    my @ocns = split($default->{data_delim}, $columns[$default->{oclc_column}]);
    my %uniq = ();
    OCN_LOOP: foreach my $ocn (@ocns) {
	my $number;

	# Conditions for failing:
	if ($default->{strict} && $ocn !~ /oc.*?\d+/i) {
	    # Under strict, only deal with ocns that at least loosely resemble oclc numbers.
	    # When, like "(orkla)555" and just plain "555" gotta go.
	    print STDERR "ocn failed strict: [$ocn]\n" if $default->{verbose};
	    next OCN_LOOP;
	} elsif ($default->{semi_strict} && $ocn =~ /^([^\d]+)/) {
	    # under semi_strict, allow plain numbers but not any non-oclc prefixes
	    my $prefix = $1;
	    if ($prefix !~ /oc/i) {
		print STDERR "bad prefix ($prefix), failed semi_strict: [$ocn]\n" if $default->{verbose};
		next OCN_LOOP;
	    }
	}

	if ($ocn =~ m/(\d+)/i) {
	    # Get the (first) numeric part out.
	    $number = $1;
	    $number =~ s/^0+//;
	    $ocn    =~ s/\s//g;
	    # delete any trailing crap e.g. (OCoLC)38150217kkh3/4/99 -> (OCoLC)38150217
	    $ocn =~ s/(\d)\D+.*/$1/;
	    # One number can only map to one surface representation.
	    print "$number => $ocn\n" if $default->{verbose};
	    $uniq{$number} = $ocn;
	} else {
	    print STDERR "[$ocn] lacks  numbers\n" if $default->{verbose};
	    next OCN_LOOP;
	}
    }
    # Return one surface representation per number in %uniq.
    $columns[$default->{oclc_column}] = join(';', sort grep { /\d/ } values %uniq);
    # Put line back together
    my $new_line = join($default->{column_delim}, @columns);
    if ($default->{verbose} && $old_line ne $new_line) {
	print STDERR "-$old_line\n";
	print STDERR "+$new_line\n\n";
    }
    print $new_line . "\n";
}
