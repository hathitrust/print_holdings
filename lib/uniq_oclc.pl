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

=cut

# Defaults:
my $column_delim = "\t";
my $data_delim   = ",";
my $oclc_column  = 0;
my $verbose      = 0;

# Override defaults with --option_name:
my ($column_delim_input) = map {/^--column_delim=(.+)/ ? $1 : ()} @ARGV;
$column_delim = $column_delim_input if $column_delim_input;

my ($data_delim_input) = map {/^--data_delim=(.+)/ ? $1 : ()} @ARGV;
$data_delim = $data_delim_input if $data_delim_input;

my ($oclc_column_input) = map {/^--oclc_column=(.+)/ ? $1 : ()} @ARGV;
$oclc_column = $oclc_column_input if $oclc_column_input;

my $verbose_input = map {/^--verbose/ ? $1 : ()} @ARGV;
$verbose = $verbose_input if $verbose_input;

print STDERR "column_delim:[$column_delim]\n";
print STDERR "data_delim:[$data_delim]\n";
print STDERR "oclc_column:[$oclc_column]\n";
print STDERR "verbose:[$verbose]\n";

# Filter out --option_name from ARGV.
@ARGV = grep {$_ !~ /^--/} @ARGV;

while (<>) {
    my $line = $_;
    chomp $line;
    my $old_line = $line;
    $line =~ s/\"//g;
    # Split line into columns
    my @columns = split($column_delim, $line);
    my @ocns    = split($data_delim, $columns[$oclc_column]);
    my %uniq    = ();
    OCN_LOOP: foreach my $ocn (@ocns) {
	# Only deal with columns that loosely resemble oclc numbers.
	# Strictly numeric columns can, after some sampling and experimentation, not be trusted.
	# Get the numeric ocn out.
	my $number;
	if ($ocn =~ m/oc.+(\d+)/i) {
	    $number = $1;
	} else {
	    print STDERR "Rejected $ocn\n" if $verbose;
	    next OCN_LOOP;
	}
	# strip leading zeroes.
	$number =~ s/^0+//;
	$ocn   =~ s/\s//g;
	# One number can only map to one surface representation.
	$uniq{$number} = $ocn;
    }
    # Return one surface representation per number in %uniq.
    $columns[$oclc_column] = join($data_delim, sort values %uniq);
    # Put line back together
    my $new_line           = join($column_delim, @columns);
    if ($verbose && $old_line ne $new_line) {
	print STDERR "-$old_line\n";
	print STDERR "+$new_line\n";
    }
    print $new_line . "\n";
}
