require 'hathidb';
require 'hathilog';

log = Hathilog::Log.new();
log.d("Started");

db = Hathidb::Db.new();
conn = db.get_conn();

# So we can measure progress.
def count_unclaimed (conn, log)
  q = "SELECT COUNT(*) AS c FROM holdings_htitem_htmember_jn WHERE member_id = ''";
  log.d(q);
  conn.query(q) do |r|
    log.d(r[:c]);
  end
end

# Check initial count.
log.d("Unclaimed count before update:");
count_unclaimed(conn, log);

# Set member_id.
update_sql = "UPDATE holdings_htitem_htmember_jn SET member_id = ? WHERE volume_id LIKE ? AND member_id = ''";
update_pq  = conn.prepare(update_sql);

# Map of prefix->member_id
prefix_member = {
  'keio' => 'ht',
};

# Do update.
prefix_member.each_pair do |prefix, member_id|
  todo = "#{update_sql} /* ?: (#{member_id}, #{prefix}.%) */";
  log.d(todo);
end

# Check final count.
log.d("Unclaimed count after update:");
count_unclaimed(conn, log);

log.d("Finished");
