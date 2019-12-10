require 'hathidata';
require 'hathidb';

db = Hathidb::Db.new();
conn = db.get_conn();

member_id = ARGV.shift;
phase     = ARGV.shift;

phases = {
  "1" => "2017-09-30",
  "2" => "2019-02-28"
}

if !phases.key?(phase) then
  raise "phase #{phase} not ok, only accepting #{phases.keys.join('/')}";
end

hdout = Hathidata::Data.new("reports/shared_print/#{member_id}_phase_#{phase}_$ymd.tsv").open('w');

sql = %w<
  SELECT 
  spc.local_id, spc.local_oclc, spc.local_bib_id, spc.local_item_id, spc.oclc_symbol, 
  spc.local_item_location, spc.local_shelving_type, spc.lending_policy, spo.sp_program
  FROM shared_print_commitments AS spc
  LEFT JOIN shared_print_other AS spo ON (spc.other_commitment_id = spo.id)
  WHERE spc.member_id = ? AND spc.committed_date = ?
>.join(' ')
q = conn.prepare(sql);

header = true;

q.enumerate(member_id, phases[phase]) do |row|
  if header then
    hdout.file.puts row.to_h.keys.join("\t");
    header = false;
  end
  hdout.file.puts row.to_a.join("\t");
end

conn.close();
hdout.close();
