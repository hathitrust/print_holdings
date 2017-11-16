use strict;
use warnings;

=pod

Assumes a tab-separated file as 1st input.
Accepts a column number (0-indexed) as 2nd input, defaults to 0 if absent.
Outputs a uniq:ed file, keyed on the given column.
It's like you could specify the uniq-key for sort -u, essentially.

Example: cols.tsv contains
a       1
a       2
b       1
b       3

$ perl sort_uf.pl cols.tsv 0
a       2
b       3

$ perl sort_uf.pl cols.tsv 1
b       1
a       2
b       3

=cut

my $f = shift || die "Need file as 1st input\n";
my $c = shift || 0;
my %uniq;
open (F, $f) || die "Cannot open file $f\n";
# Read file line by line.
while (<F>) {
    my $line = $_;
    chomp $line;
    # Split line into cols.
    my @cols = split("\t", $line);
    # Store line keyed on column $c if if exists.
    $uniq{$cols[$c]} = $line if defined $cols[$c];
}
close(F);

# Output lines sorted by key.
foreach my $k (sort keys %uniq) {
    print $uniq{$k} . "\n";
}
