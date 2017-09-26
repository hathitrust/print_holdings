use strict;
use warnings;

=pod

member_id
local_oclc
resolved_oclc
local_id
local_bib_id
local_item_id
oclc_symbol
local_item_location
local_shelving_type
other_commitments
lending_policy
scanning_repro_policy
ownership_history
Print Holdings Review fields (maybe add this separately after XML extract)
overlap_ht
overlap_sp
overlap_group
gov_doc
language: 008 fixed field position 35-37
pub_year: 008 fixed field positions 7-10
pub_country: 008 fixed field positions 15-17
call_no: 050 a

=cut
my $header = 1;
while (<>) {
    my $line = $_;
    next if $line !~ /\t/;
    chomp $line;
    my (
	$resolved_oclc, $lending_policy, $local_bib_id, $local_id,
	$local_item_id, $local_item_location, $local_oclc, $local_shelving_type,
	$member_id, $oclc_symbol, $other_commitments, $overlap_ht, $overlap_sp,
	$overlap_group, $gov_doc, $language, $pub_year, $pub_country, $callno
    ) = split("\t", $line);
    my ($scanning_repro_policy, $ownership_history) = ('', '');
    my ($other_sp_program, $retention_end_date)     = ('', '');
    if ($header) {
	($scanning_repro_policy, $ownership_history) = ('scanning_repro_policy', 'ownership_history');
	($other_sp_program, $retention_end_date) = ('(other_sp_program)', '(retention_end_date)');
	$header = 0;
    } else {
	# If there's a value in other_commitments, split out into program and date
	if ($other_commitments eq '') {
	    $other_commitments = 'N';
	} else {
	    ($other_sp_program, $retention_end_date) = split(':', $other_commitments);
	    $other_sp_program   =~ s/\s//g;
	    $retention_end_date =~ s/\s//g;
	    $other_commitments = 'Y';

	    # Upcase all the upcaseable stuff;
	    map {$_ =~ tr/[a-z]/[A-Z]/} (
		$lending_policy,
		$local_shelving_type,
		$oclc_symbol
	    );
	}
    }

    print join(
	"\t",
	$member_id, $local_oclc, $resolved_oclc, $local_id, $local_bib_id, $local_item_id, $oclc_symbol, # ids
	$local_item_location, $local_shelving_type, $other_commitments, $other_sp_program, $retention_end_date, $lending_policy, # meta stuff
	$scanning_repro_policy, $ownership_history, # all empty
	$overlap_ht, $overlap_sp, $overlap_group, # overlaps
	$gov_doc, $language, $pub_year, $pub_country, $callno # stuff from xml
    ) . "\n";
}
