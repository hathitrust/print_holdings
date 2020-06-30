use strict;
use warnings;

# May need to do strict uniq-oclc on a member, and potentially good ones fail because no oc prefix.
# $face->{palm};
# Like I want to filter out the bad-prefix ones: ORK555, BLA123, OOPS567, 3029182

while (<>) {
    my $line = $_;
    my @cols = split("\t", $line);
    if ($cols[0] !~ /oc/i) {
	$cols[0] = "oc" . $cols[0];
    }
    print join("\t", @cols);
}
