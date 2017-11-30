use strict;
use warnings;

=pod

Given a file as input, this script will output a file uniqued on the zeroth column.
Change the column with -f=<column_number>.
Change the column separator (default "\t") with -F=<column_separator>.

Example: cols.tsv contains
a	1
a	2
b	1
b	3

$ perl sort_uf.pl cols.tsv -f=0
a	2
b	3

$ perl sort_uf.pl cols.tsv -f=1
b	1
a	2
b	3

=cut

my %flags = (
    'f' => 0,	 # which field to key on
    'F' => "\t", # field separator
);


my $i = 0;
# Separate files from flags.
foreach my $arg (@ARGV) {
    if ($arg =~ m/^-(.+?)=(\S+)/) {
	my ($command, $value) = ($1, $2);
	$flags{$command} = $value;
	delete $ARGV[$i];
    } else {
	$i++;
    }
}

my %uniq;
# Read input line by line.
while (<>) {
    my $line = $_;
    chomp $line;
    # Split line into cols.
    my @cols = split($flags{'F'}, $line);
    # Store line keyed on column -f if if exists.
    $uniq{$cols[$flags{'f'}]} = $line if defined $cols[$flags{'f'}];
}

# Output lines sorted by key.
foreach my $k (sort keys %uniq) {
    print $uniq{$k} . "\n";
}
