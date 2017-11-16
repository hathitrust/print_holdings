require 'hathidb';
require 'hathiquery';

db   = Hathidb::Db.new();
conn = db.get_conn();
conn.query(Hathiquery.get_active_members) do |row|
  puts row[:member_id];
end
conn.close();
