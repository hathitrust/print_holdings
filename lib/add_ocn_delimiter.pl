use strict;
use warnings;

# Sometimes the ocns are just delimited with space, and the scrub
# script will just smush those together into one giant ocn.
# Use this to insert semicolons as delimiters.

while (<>) {
    my $line = $_;
    chomp $line;

    my ($ocn_str, @rest) = split("\t", $line);
    next if !defined $ocn_str;
    $ocn_str =~ s/\s+/;/g;
    print join("\t", ($ocn_str, @rest)) . "\n";
}
