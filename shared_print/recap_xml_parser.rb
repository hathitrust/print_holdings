# coding: utf-8

=begin

https://wush.net/jira/hathitrust/browse/HTP-1080

– Owning institution ID (e.g. CUL)
– Owning institution BibID
– 035 a (OCLC number)
– 876 p (bar code)
– 876 h (looks like lending restriction, e.g. In-Library Use)
– 900 a (Shared) preferably include only those with Shared in the 900a)

=end

require 'nokogiri';
require 'hathidata';
require 'hathilog';

path = ARGV.shift;
hdin = Hathidata::Data.new(path).open('r');

hdout = Hathidata::Data.new(
  path.sub(/\.xml$/, "_parsed_$ymd.tsv")
).open('w');
doc  = Nokogiri::XML(hdin.file);
log  = Hathilog::Log.new();

owner_map = {
  'CUL'  => {'member_id' => 'columbia',  'oclc_symbol' => 'ZCU'},
  'NYPL' => {'member_id' => 'nypl',      'oclc_symbol' => 'NYP'},
  'PUL'  => {'member_id' => 'princeton', 'oclc_symbol' => 'PUL'}
};

val_template = {
  'lending_policy'      => '',
  'local_bib_id'        => '',
  'local_id'            => '',
  'local_item_id'       => '',
  'local_item_location' => '', # lizanne may get a value
  'local_oclc'          => [],
  'local_shelving_type' => 'SFCAHM',
  'member_id'           => '',
  'oclc_symbol'         => '',
  'other_commitments'   => 'recap : 2099-12-31', # lizanne may get a value for date
};
header = val_template.keys.sort;
# Print header.
hdout.file.puts(header.join("\t"));

doc.xpath('//bibRecord').each do |node|
  extracted_vals = val_template.clone;
  extracted_vals['local_oclc'] = []; # This does not reset when i clone?
  # Only get those where 900a == shared
  shared = false;
  node.xpath('descendant::datafield[@tag="900"]/subfield[@code="a"]').each do |tag_900a|
    if tag_900a.text.downcase.strip == 'shared' then
      shared = true;
    end
  end

  if shared == true then
    # Get member_id and oclc_symbol based on owningInstitutionId
    node.xpath('descendant::owningInstitutionId').each do |owner|
      if owner_map.has_key?(owner.text) then
        extracted_vals['member_id']   = owner_map[owner.text]['member_id'];
        extracted_vals['oclc_symbol'] = owner_map[owner.text]['oclc_symbol'];
      else
        log.w("Unknown owner #{owner.text}");
      end
    end

    # Get local_id and local_bib_id based on owningInstitutionBibId
    node.xpath('descendant::owningInstitutionBibId').each do |bibId|
      extracted_vals['local_id']     = bibId.text;
      extracted_vals['local_bib_id'] = bibId.text;
    end

    # Get oclc numbers
    # local_oclc - 035 a , if also matching regex
    node.xpath('descendant::datafield[@tag="035"]/subfield[@code="a"]').each do |tag_035a|
      possible_oclc = tag_035a.text;
      if possible_oclc =~ /^(\(OCoLC\))?(oc.+)?\d+/ then
        extracted_vals['local_oclc'] << possible_oclc;
      else
        log.w("not an oclc: #{possible_oclc}");
      end
    end

    # Get local_item_id
    # local_item_id: 876 p (bar code)
    node.xpath('descendant::datafield[@tag="876"]/subfield[@code="p"]').each do |tag_876p|
      extracted_vals['local_item_id'] = tag_876p.text;
    end

    # Get lending_policy
    # If 876h is 'In-Library Use' then BLO
    node.xpath('descendant::datafield[@tag="876"]/subfield[@code="h"]').each do |tag_876h|
      if tag_876h.text =~ /in.library.use/i then
        extracted_vals['lending_policy'] = 'BLO'
      end
    end

    extracted_vals['local_oclc'] = extracted_vals['local_oclc'].join("; ");
    hdout.file.puts(header.map{|k| extracted_vals[k] == '' ? '' : extracted_vals[k] }.join("\t"));
  end
end
hdin.close();
hdout.close();
