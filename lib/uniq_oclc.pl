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
  --data_delim
  --oclc_column
  --verbose

=cut

my $default = {
    column_delim => "\t",
    data_delim   => ",",
    oclc_column  => 0,
    verbose      => 0,
};

# Look for default overrides in @ARGV and set accordingly.
foreach my $k (keys %$default) {
    my ($input) = map {/^--$k=(.+)/ ? $1 : ()} @ARGV;
    $default->{$k} = $input if $input;
    print STDERR "$k:[$default->{$k}]\n";
}

# Filter out --option_name from ARGV.
@ARGV = grep {$_ !~ /^--/} @ARGV;

while (<>) {
    my $line = $_;
    chomp $line;
    my $old_line = $line;
    $line =~ s/\"//g;
    # Split line into columns
    my @columns = split($default->{column_delim}, $line);
    my @ocns    = split($default->{data_delim}, $columns[$default->{oclc_column}]);
    my %uniq    = ();
    OCN_LOOP: foreach my $ocn (@ocns) {
	# Only deal with ocns that loosely resemble oclc numbers.
	# Strictly numeric ocns can, after some sampling and experimentation, not be trusted.
	# Get the numeric ocn out.
	my $number;
	if ($ocn =~ m/oc.+(\d+)/i) {
	    $number = $1;
	} else {
	    print STDERR "Rejected $ocn\n" if $default->{verbose};
	    next OCN_LOOP;
	}
	# strip leading zeroes.
	$number =~ s/^0+//;
	$ocn   =~ s/\s//g;
	# One number can only map to one surface representation.
	$uniq{$number} = $ocn;
    }
    # Return one surface representation per number in %uniq.
    $columns[$default->{oclc_column}] = join($default->{data_delim}, sort values %uniq);
    # Put line back together
    my $new_line = join($default->{column_delim}, @columns);
    if ($default->{verbose} && $old_line ne $new_line) {
	print STDERR "-$old_line\n";
	print STDERR "+$new_line\n";
    }
    print $new_line . "\n";
}
