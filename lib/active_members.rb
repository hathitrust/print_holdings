require 'hathidb';
require 'hathiquery';

db   = Hathidb::Db.new();
conn = db.get_conn();
conn.query(Hathiquery.get_active_members) do |row|
  puts row[:member_id];
end
conn.close();

# For if you want to add a non-active member as an active member.
# WHY ON EARTH? Because ucm. I break other things if I set ucm to
# be active, so I'd rather do this.
ARGV.each do |arg|
  if arg =~ /add=(\w+)/ then
    puts $1;
  end
end
