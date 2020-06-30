use strict;
use warnings;

=pod

When you have a line of holdings like:

(OCoLC)24521672;(OCoLC)960309 <TAB> 9918771910102001 <TAB> 1

... and the ocns are clearly different works:

https://www.worldcat.org/oclc/24521672
https://www.worldcat.org/oclc/960309

... and the library clearly holds both,
then just split the record, into:

(OCoLC)24521672 <TAB> 9918771910102001 <TAB> 1
(OCoLC)960309   <TAB> 9918771910102001 <TAB> 1

=cut

my $ocn_col_i = 0; # Which column has the ocn, edit as appropriate
my $col_delim = "\t";
my $ocn_delim = qr/[ ,:;]+/;

while (<>) {
    my $line = $_;
    chomp $line;
    my @cols = split($col_delim, $line);
    my @ocns = split($ocn_delim, $cols[$ocn_col_i]);

    foreach my $ocn (@ocns) {
	$cols[$ocn_col_i] = $ocn;
	print join($col_delim, @cols) . "\n";
    }
}
