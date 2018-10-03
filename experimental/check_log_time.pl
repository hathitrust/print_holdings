use strict;
use warnings;
use Time::Piece;
use Time::Seconds;

=pod

Read a logfile from stdin, and look at the first and last timestamp
(YYYY-MM-DD HH:MM:SS format only). Print the diff in time between
first and last timestamp.

=cut

my $start_time = undef;
my $end_time   = undef;
my $ts_rx      = qr/(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d)/;

while (<>) {
    my $line = $_;
    if ($line =~ $ts_rx) {
	my $time    = $1;
	$start_time = $time if !defined $start_time;
	$end_time   = $time;
    }

    if (eof) { # Do this at the end of each file (you can pass in multiple).
	print "$ARGV\n";
	if (!$start_time || !$end_time) {
	    print "Start time and/or end time missing.\n";
	    next;
	}

	my $mask     = "%Y-%m-%d %H:%M:%S";
	my $tp_start = Time::Piece->strptime($start_time, $mask);
	my $tp_end   = Time::Piece->strptime($end_time,   $mask);
	my $diff     = $tp_end - $tp_start;

	print "Started: $start_time, ended: $end_time\n";
	print "Duration: " . $diff->pretty . "\n";
	print "---\n";
	$start_time = undef;
    }
}


