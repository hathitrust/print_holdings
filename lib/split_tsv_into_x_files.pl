use strict;
use warnings;

=pod

Takes a .tsv and a chunk size as input.
Chunks the infile into sequentially numbered outfiles
with line count no greater than chunk size.

It repeats the header (first non-blank line in the input file)
for each output file.

$ wc -l example.txt

  100

$ perl split_tsv_into_x_files.pl example.tsv 25
$ wc -l example.*

  100 example.tsv         (1 header line and 99 body lines)
    25 example.tsv.000001 (1 header line and 24 body lines)
    25 example.tsv.000002 (1 header line and the next 24 body lines)
    25 example.tsv.000003 (1 header line and the next 24 body lines)
    25 example.tsv.000004 (1 header line and the next 24 body lines)
     4 example.tsv.000005 (1 header line and the last 3 body lines)

=cut

my $infile     = shift @ARGV      || die "filename as 1st arg\n";
my $chunk_size = int(shift @ARGV) || die "chunk size as 2nd arg\n";
my $count_outfile = 1;

if ($chunk_size <= 1) {
    die "That would be a bad idea.\n";
}

my $header = '';
open(my $fh, $infile) || die "cannae open file infile $infile\n";

my $i = 0;
my $outfile = get_outfile($count_outfile);
while (<$fh>) {
    my $line = $_;
    chomp $line;
    next if $line eq '';
    if (!$header) {
	$header = $line;
	print $outfile $header . "\n";
	$i++;
	next;
    }
    if ($i >= $chunk_size) {
	close($outfile);
	$outfile = get_outfile(++$count_outfile);
	print $outfile $header . "\n";
	$i = 1;
    }
    print $outfile $line . "\n";
    $i++;
}
close($fh);
close($outfile);

sub get_outfile {
    my $count_outfile = shift;
    my $outfile_n = "$infile." . sprintf("%06d", $count_outfile);
    open(my $outf, '>', $outfile_n) || die "cannae open outfile $outfile_n\n";
    return $outf;
}
