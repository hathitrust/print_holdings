require 'hathidb';

# Get the number of shared print members holding a resolved_oclc. Pass args or pipe in.
# ruby get_sp_h.rb 555
# OR
# echo "555" | ruby get_sp_h.rb

db   = Hathidb::Db.new();
conn = db.get_conn();
sql  = "SELECT COUNT(DISTINCT member_id) AS sp_h FROM shared_print_commitments WHERE resolved_oclc = ?";
q    = conn.prepare(sql);

argx = ARGV;
if argx.empty? then
  argx = ARGF;
end

argx.each do |line|
  ocn = line.strip;
  q.enumerate(ocn) do |row|
    puts "#{ocn}\t#{row[:sp_h]}";
  end
end

conn.close();
