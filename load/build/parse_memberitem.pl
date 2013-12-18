use strict;
use warnings;

open(F, "memberitem.20121022.sql") || die "Cannot open\n$!\n";
my $i = 0;

my $ins_rx  = qr/^INSERT INTO/;
my $stmt_rx = qr/(\(.+?\))/;
my $the_rx  = qr/'madrid'/;

while (<F>) {
    print "$i\n" if ++$i % 100 == 0;
    my $line = $_;
    chomp $line;
    # There are very long lines, beginning wiht 'INSERT INTO'
    if ($line =~ $ins_rx) {
	# Then a bunch of (...),(...),(...), ...
	foreach my $m (($line =~ /$stmt_rx/g)) {
	    # We are interested in each () containing the_rx
	    if ($m =~ $the_rx) {
		print $m . "\n"
	    }
	}
    }
}
close(F);
