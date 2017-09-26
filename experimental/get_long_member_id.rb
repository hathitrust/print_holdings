require 'hathidb';

# Check if any members have local_id length equal to column max length. If so, they might have sent us a longer id that got truncated.

db   = Hathidb::Db.new();
conn = db.get_conn();

members_q  = conn.prepare("SELECT member_id FROM holdings_htmember WHERE status=1 ORDER BY member_id");
local_id_q = conn.prepare("SELECT local_id FROM holdings_memberitem WHERE member_id = ? AND LENGTH(local_id) = 50 LIMIT 0,3");
members_q.enumerate() do |rowx|
  member_id = rowx[:member_id];  
  puts member_id;
  local_id_q.enumerate(member_id) do |rowy|
    puts "\t" + rowy[:local_id];
  end
end
