require 'hathidb';
require 'hathilog';

db   = Hathidb::Db.new();
conn = db.get_conn();
log  = Hathilog::Log.new();
log.i("Started");

# Get volume_ids belonging to serial clusters where
# the holder has copy_count or access_count gt 1.
get_serial_volids_sql = %w{
  SELECT DISTINCT
  t3.volume_id, t3.member_id, t3.copy_count, t3.access_count
  FROM holdings_cluster                    AS t1
  NATURAL JOIN holdings_cluster_htitem_jn  AS t2
  NATURAL JOIN holdings_htitem_htmember_jn AS t3
  WHERE t1.cluster_type = 'ser'
  AND (t3.copy_count > 1 OR t3.access_count > 1)
}.join(' ');
get_serial_volids_q = conn.prepare(get_serial_volids_sql);

# Set copy_count to 1 and access_count to 0/1.
update_counts_sql = %w{
  UPDATE holdings_htitem_htmember_jn
  SET copy_count = 1, access_count = ?
  WHERE volume_id = ? AND member_id = ?
}.join(' ');
update_counts_q = conn.prepare(update_counts_sql);

get_serial_volids_q.enumerate() do |row|
  puts "-\t#{row.to_a.join("\t")}";
  volume_id    = row[:volume_id];
  member_id    = row[:member_id];
  copy_count   = row[:copy_count].to_i;
  access_count = row[:access_count].to_i;
  # If access_count was anything, set to 1, otherwise 0.
  new_acc = 0;
  new_acc = 1 if access_count > 0;
  update_counts_q.execute(new_acc, volume_id, member_id);
  puts ['+', volume_id, member_id, 1, new_acc].join("\t");
  puts "#";
end

log.i("Finished");
