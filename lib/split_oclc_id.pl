use strict;
use warnings;

=pod

Usage:

  perl split_oclc_id.pl -f=<input_file> -o=<oclc_col> -i=<id_col>

Turn e.g.:

  1661467;1661558;7805492;589197  b11087018;b11087031;b12243474;b23383768 CH              16/1971         0

... into:

  1661467 b11087018       CH              16/1971         0
  1661558 b11087031       CH              16/1971         0
  7805492 b12243474       CH              16/1971         0
  589197  b23383768       CH              16/1971         0

=cut

# Default settings, some must be overridden.
my $settings = {
    '-f' => undef, # Input file
    '-o' => undef, # oclc column number (0-indexed)
    '-i' => undef, # local id column number (0-indexed)
    '-t' => "\t",  # col delimiter in input file
    '-d' => ";",   # data delimiter inside column
};

# Set settings.
foreach my $arg (@ARGV) {
    if ($arg =~ m/^(-\w)=(.+)$/) {
	my $sk = $1;
	my $sv = $2;
	print STDERR "$sk set to $sv\n";
	$settings->{$sk} = $sv;
    }
}

# Check for required settings that weren't set.
foreach my $sk (sort keys %$settings) {
    my $sv = $settings->{$sk};
    if (!defined $sv) {
	print STDERR "Setting $sk is not defined.\n";
	die;
    }
}

# Go through input file
open(F, $settings->{'-f'}) or die "Cannot open infile\n";
while (<F>) {
    my $line = $_;
    chomp $line;
    my @cols     = split($settings->{'-t'}, $line);
    my $oclc_str = $cols[$settings->{'-o'}];
    my $id_str   = $cols[$settings->{'-i'}];
    if ($oclc_str =~ m/$settings->{'-d'}/ && $id_str =~ m/$settings->{'-d'}/) {
	my @oclcs = split($settings->{'-d'}, $oclc_str);
	my @ids   = split($settings->{'-d'}, $id_str);
	my @cols_copy = @cols;
	while (@oclcs && @ids) {
	    my $oclc = shift @oclcs;
	    my $id   = shift @ids;
	    map {$_ =~ s/"//g} ($oclc, $id); # clean
	    $cols_copy[$settings->{'-o'}] = $oclc;
	    $cols_copy[$settings->{'-i'}] = $id;
	    print join($settings->{'-t'}, @cols_copy) . "\n";
	}

	if (scalar(@oclcs) != scalar(@ids)) {
	    print STDERR "Uneven number of oclcs/ids in line:\n$line\n";
	}
    } else {
	print $line . "\n";
    }
}
close(F);
