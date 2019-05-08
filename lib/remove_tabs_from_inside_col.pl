use strict;
use warnings;

=pod

When you asked for a .tsv and they give you a .csv, so you do:

  $ tr ',' '\t'

... but realize that the data has quoted strings in it, containing commas.

So now you need to stitch it back together.

=cut

# rx for a pair of quotes with a tab inside, somewhere.
my $rx = qr/("[^"]+\t[^"]+")/;

while (<>) {
    my $line = $_;
    chomp $line;
    # matches e.g. ('"Presley    Elvis"')
    my @matches = ($line =~ m/$rx/);
    foreach my $m (@matches) {
	my $tabless = $m;
	# tabless e.g. '"Presley,Elvis"'
	$tabless =~ s/\t/,/g;
	print STDERR "Fixing: [$m] -> [$tabless]\n";
	$line =~ s/\Q$m/$tabless/;
    }
    print $line . "\n";
}
