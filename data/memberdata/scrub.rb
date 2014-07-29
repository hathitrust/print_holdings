require 'member_scrubber';

# Copy this script into each subdirectory and edit the _cols hashes
# according to the member data. Sometimes the hash given to 
# MemberScrub::MemberScrubber.new needs to be edited too.

if ARGV.length != 3
  abort "Usage: ruby scrub.rb <member_id> <type>(mono|multi|serial) <infile>\n";
end

member_id = ARGV[0];
type      = ARGV[1];
infile    = ARGV[2];
outfile   = "HT003_#{member_id}.#{type}.tsv";

mono_cols = {
  :oclc      => 0,
  :local_id  => 1,
  :status    => 2,
  :condition => 3,
};

multi_cols = {
  :oclc       => 0,
  :local_id   => 1,
  :status     => 2,
  :condition  => 3,
  :enum_chron => 4,
};

serial_cols = {
  :oclc     => 0,
  :local_id => 1,
  :issn     => 2,
};

if type == "mono" then
  mapper = MemberScrub::ColumnMapping.new(mono_cols);
  scrubber = MemberScrub::MemberScrubber.new(
	  :member_id    => member_id,
	  :mapper       => mapper,
	  :data_type    => 'mono',
	  :header_lines => 0,
	  :min_cols     => 3);
elsif type == "multi" then
  mapper = MemberScrub::ColumnMapping.new(multi_cols);
  scrubber = MemberScrub::MemberScrubber.new(
	  :member_id    => member_id,
	  :mapper       => mapper,
	  :data_type    => 'multi',
	  :header_lines => 0,
	  :min_cols     => 3);
elsif type == "serial" then
  mapper = MemberScrub::ColumnMapping.new(serial_cols);
  scrubber = MemberScrub::MemberScrubber.new(
	  :member_id    => member_id,
	  :mapper       => mapper,
	  :data_type    => 'serial',
	  :header_lines => 0,
	  :min_cols     => 2);
else
  abort "unknown type: " + type;
end

scrubber.process_data(infile, outfile);
