use strict;
use warnings;

=pod

Sanity check after the load script has loaded all files.

Takes 2 files:

One the looks like:

 HT003_vanderbilt.multi.tsv
 HT003_vanderbilt.serial.tsv
 HT003_vt.mono.bad.tsv
 HT003_vt.mono.tsv

... and one that looks like:

(select member_id, count(member_id) from
 holdings_memberitem group by member_id)

 | unl        |          2660077 |
 | uom        |          6388722 |
 | upenn      |          3531307 |
 | uq         |          1064353 |

Makes sure that all member_ids in the first file
are also represented in the second.

=cut

my $should_be_loaded = {};

open(F1, "ht03_contents.txt") || die $!;
while (<F1>) {
    my $line = $_;
    chomp $line;
    if ($line =~ /HT003_(.+).(mono|multi|serial)\.tsv/) {
	my $member_id = $1;
	my $item_type = $2;
	$should_be_loaded->{$member_id} ||= {};
	$should_be_loaded->{$member_id}->{$item_type} = 1;
    } else {
	print "No match on F1 line $line\n";
    }
}
close(F1);

my $was_loaded = {};
my $allcount   = 0;
open(F2, "got_loaded.txt") || die $!;
while (<F2>) {
    my $line = $_;
    chomp $line;
    if ($line =~ /([a-z]+).+?(\d+)/) {
	my $member_id = $1;
	$member_id =~ s/\s//g;
	my $count = $2;
	$allcount += $count;
	$was_loaded->{$member_id} = $count;
    } else {
	print "No match on F2 line $line\n";
    }
}
close(F2);


foreach my $m (sort keys %$should_be_loaded) {
    my $w = $was_loaded->{$m} || 'NO';

    print "$m\t$w\t";
    print join("\t", map {$should_be_loaded->{$m}->{$_} ? $_ : 'x'} qw/mono multi serial/);
    print "\n";
}

print "Allcount $allcount\n";
