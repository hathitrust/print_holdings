use strict;
use warnings;

=pod

If you have one big tsv file and want to split it into several
based on the value of one of the columns. Say you have the file:

  a\t1
  b\t1
  a\t2
  b\t2

Run:

  $ perl split_tsv_by_col.pl 0 yourfile.tsv

... to get:

  == a.tsv ==
  a\t1
  a\t2

  == b.tsv ==
  b\t1
  b\t2

=cut

# Which col in the input file is the key?
my $key_col = shift @ARGV;
if ($key_col !~ m/^\d+$/) {
    die "need key_col (int) as 1st arg\n";
}

# Are we getting a header line from input?
my $header      = 0;
my $header_line = '';
if (grep {$_ eq '--header'} @ARGV) {
    $header = 1;
    @ARGV   = grep {$_ ne '--header'} @ARGV;
}

my $files = {};

while (<>) {
    my $line = $_;

    # If we're doing header, save first line in the input file for later and skip
    if ($header) {
	$header_line = $line;
	$header      = 0;
	next;
    }

    # Split cols and get key col
    my @cols = split("\t", $line);
    my $key = $cols[$key_col];
    $key =~ s!/!-!g;
    if ($key !~ /\w+/) {
	print STDERR "Bad key col in line ($line)\n";
	next;
    }

    # Get file given key
    my $file = $files->{$key};

    # Open new file if not previously opened.
    if (!$file) {
	open(my $fh, ">$key.tsv") || die "cannot open file $key.tsv for writing\n";
	print STDERR "Opening $key.tsv\n";
	$files->{$key} = $fh;
	$file = $fh;
	# Print header if we're doing that.
	if ($header_line) {
	    print $file $header_line;
	}
    }

    # Print line to the right file.
    print $file $line;
}

# Close all the files.
foreach my $fh (keys %$files) {
    print STDERR "Closing $fh.tsv\n";
    close($fh);
}
