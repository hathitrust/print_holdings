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
  "SELECT oclc_x FROM oclc_resolution WHERE oclc_y = ?"
);
oclc_res_q.enumerate(oclc) do |row|
  resolved_oclc = row[:oclc_x].to_i;
  check_alternates = true;
  alternates << resolved_oclc;
end
puts "Resolved OCN for #{oclc} is #{resolved_oclc}";
puts "---";

# Get alternate forms.
alternates_q = conn.prepare(%w{
  SELECT oclc_y FROM oclc_resolution WHERE oclc_x = ?
}.join(' '));
if check_alternates then
  alternates_q.enumerate(resolved_oclc) do |row|
    alternates << row[:oclc_y].to_i;
  end
end
alternates.uniq!
puts "Alternate forms: #{alternates.join(',')}"
puts "---";

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
  SELECT DISTINCT member_id FROM shared_print_commitments WHERE resolved_oclc = ? ORDER BY member_id
}.join(' '));
sp_q.enumerate(resolved_oclc) do |row|
  puts row.to_a.join("\t");
end
puts "---";
