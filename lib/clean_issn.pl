use strict;
use warnings;

=pod

If a serials file contains messy issn values, use this script
to clean them so only DDDD-DDD[DX] matches remain.

Example:

  $ perl clean_issn.pl messy_serials.tsv > clean_serials.tsv

=cut

my $col_delim  = "\t";
my $issn_col   = 3; # << edit as appropriate
my $issn_regex = qr/(\d{4}-\d{3}[0-9Xx])/;

while (<>) {
    my $line = $_;
    chomp $line;
    my @cols = split($col_delim, $line);
    if ($cols[$issn_col]) {
	my (@issns) = $cols[$issn_col] =~ /$issn_regex/g;
	$cols[$issn_col] = join(";", @issns);
	print join($col_delim, @cols) . "\n";
    } else {
	print $line . "\n";
    }
}
