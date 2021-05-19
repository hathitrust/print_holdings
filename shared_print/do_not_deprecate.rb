=begin

When a commitment does not match holdings, but the member says they still hold it.

ruby do_not_deprecate.rb <member_id> <id_type> <input_file>

=end

require 'hathidata';
require 'hathidb';

id_types = %w[local_id local_oclc both];

member_id = ARGV.shift;

# must be a member of id_types
id_type   = ARGV.shift;

# File with local_ocns or local_ids that should be set to do_not_deprecate=1
dnd_file  = ARGV.shift; 

if
  member_id.nil? || id_type.nil? || dnd_file.nil? || !id_types.include?(id_type)
then
  raise "Call like so:\nruby #{__FILE__} <member_id> {#{id_types.join('|')}} <path_to_do_not_deprecate_file>";
end

db   = Hathidb::Db.new();
conn = db.get_conn();
hdin = Hathidata::Data.new(dnd_file).open('r');

# Perform updates
if id_type == 'both' then
  sql_u = "UPDATE shared_print_commitments SET do_not_deprecate = 1 WHERE member_id = ? AND local_oclc = ? AND local_id = ?";
  q_u   = conn.prepare(sql_u);
  hdin.file.each_line do |line|
    line.strip!
    cols = line.split("\t");
    local_oclc = cols[0];
    local_id   = cols[1];
    if line =~ /\d/ then
      # puts "update #{member_id} #{id_type} #{local_oclc} + #{local_id}";
      q_u.execute(member_id, local_oclc, local_id);
    end
  end
else
  sql_u = "UPDATE shared_print_commitments SET do_not_deprecate = 1 WHERE member_id = ? AND #{id_type} = ?";
  q_u   = conn.prepare(sql_u);
  hdin.file.each_line do |line|
    line.strip!
    cols = line.split("\t");
    id = cols[0];
    if id =~ /\d/ then
      # puts "update #{member_id} #{id_type} #{id}";
      q_u.execute(member_id, id);
    end
  end
end

hdin.close();

# Output report
sql_s = %w<
  SELECT member_id, local_id, local_oclc, do_not_deprecate
  FROM shared_print_commitments
  WHERE member_id = ? AND do_not_deprecate = 1
>.join(' ');
q_s = conn.prepare(sql_s);

hdout = Hathidata::Data.new("do_not_deprecate_#{member_id}_$ymd.tsv").open('w');
header = true;
q_s.enumerate(member_id) do |row|
  if header == true then
    hdout.file.puts(row.to_h.keys.join("\t"));
    header = false;
  end
  hdout.file.puts(row.to_a.join("\t"));
end
hdout.close();
