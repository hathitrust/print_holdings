require 'hathidb';

# If you need to temporarily alter bits of holdings_htitem_H, perhaps for reasons of re-estimating cost
# or for estimating merges of partially existing members.

# Bump down for a member to turn the H of all of the volumes they hold -1.
# Bump up   for a member to turn the H of all of the volumes they hold +1.

# Call like:
# ruby bump_member.rb mit down
# ruby bump_member.rb mit up

# You might want to make a backup first:
# mysqldump --quick --dump-date -h mysql-htprep -u <USERNAME> -p ht_repository holdings_htitem_H > /htapps/mwarin.babel/phdb_scripts/data/sql/backup_holdings_htitem_H_YYYYMMDD.sql.gz

db   = Hathidb::Db.new();
conn = db.get_conn();

get_volume_ids_sql = "SELECT volume_id FROM holdings_htitem_htmember_jn WHERE member_id = ?";
get_volume_ids_q   = conn.prepare(get_volume_ids_sql);

bump_down_sql = "UPDATE holdings_htitem_H SET H = H - 1 WHERE volume_id = ?";
bump_down_q   = conn.prepare(bump_down_sql);

bump_up_sql = "UPDATE holdings_htitem_H SET H = H + 1 WHERE volume_id = ?";
bump_up_q   = conn.prepare(bump_up_sql);

member_id = ARGV.shift;
direction = ARGV.shift;

if (direction == 'up' || direction == 'down') then
  i = 0;
  get_volume_ids_q.enumerate(member_id) do |row|
    i += 1;
    puts i if i % 100000 == 0;
    vid = row[:volume_id];
    case direction
    when 'up'
      bump_up_q.execute(vid);
    when 'down'
      bump_down_q.execute(vid);
    end    
  end
else
  puts "Give direction as 2nd ARG, 'up' / 'down'.";
end
