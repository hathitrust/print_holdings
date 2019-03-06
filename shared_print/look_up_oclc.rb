require 'hathidata';
require 'hathidb';

db   = Hathidb::Db.new();
conn = db.get_conn();
oclc = ARGV.shift.to_i;

resolved_oclc    = oclc;
check_alternates = false;
alternates       = [oclc];

# Do oclc resolution.
oclc_res_q = conn.prepare(
  "SELECT resolved FROM oclc_concordance WHERE variant = ?"
);
oclc_res_q.enumerate(oclc) do |row|
  resolved_oclc = row[:resolved].to_i;
  check_alternates = true;
  alternates << resolved_oclc;
end
puts "Resolved OCN for #{oclc} is #{resolved_oclc}";
puts "---";

# Get alternate forms.
alternates_q = conn.prepare(%w{
  SELECT variant FROM oclc_concordance WHERE resolved = ?
}.join(' '));
if check_alternates then
  alternates_q.enumerate(resolved_oclc) do |row|
    alternates << row[:variant].to_i;
  end
end
alternates.uniq!
if !alternates.empty? then
  puts "Alternate forms: #{alternates.join(',')}"
  puts "---";
end
  
# Who has it in their print holdings?
ph_q = conn.prepare(%W{
  SELECT member_id, item_type, status, item_condition, COUNT(*)
  FROM holdings_memberitem
  WHERE oclc IN (#{(['?'] * alternates.size).join(',')})
  GROUP BY member_id, item_type, status, item_condition
  ORDER BY member_id, item_type, status, item_condition
}.join(' '));
puts "Who holds this?";
ph_q.enumerate(*alternates) do |row|
  puts row.to_a.join("\t");
end
puts "---";

# Who has a retention commitment?
puts "Who has a commitment?";
sp_q = conn.prepare(%w{
  SELECT DISTINCT member_id, local_oclc, resolved_oclc FROM shared_print_commitments WHERE resolved_oclc = ? ORDER BY member_id
}.join(' '));
puts [:member_id, :local_oclc, :resolved_oclc].join("\t")
sp_q.enumerate(resolved_oclc) do |row|
  puts row.to_a.join("\t");
end
puts "---";
