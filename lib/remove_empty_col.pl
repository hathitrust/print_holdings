use strict;
use warnings;

=pod

Takes an infile like:

1\t\t\t\t2\t\t\t\t3
4\t\t\t\t5\t\t\t\t6
7\t\t\t\t8\t\t\t\t9

... and turns it into:

1\t2\t3
4\t5\t6
7\t8\t9

Only columns with NO contents matching \w are removed.

Writes to stdout.

Call like so:

$ perl remove_empty_col.pl loose_input.tsv > tight_output.tsv

=cut

my $infile = shift @ARGV;
# First pass, count columns with contents
my %cols_with_data;

open(F, $infile);
while (<F>) {
    my $line = $_;
    chomp $line;
    my @cols = split("\t", $line);
    foreach my $i (0..$#cols) {
	if ($cols[$i] =~ /\w/) {
	    $cols_with_data{$i}++;
	}
    }
}
close(F);

my @ok_cols = sort keys %cols_with_data;

# Second pass, only print cols with contents
open(F, $infile);
while (<F>) {
    my $line = $_;
    chomp $line;
    my @cols = split("\t", $line);
    print join("\t", @cols[@ok_cols]) . "\n";
}
close(F);
